import datetime
import time
import clickhouse_connect

from logging import getLogger
from dataclasses import dataclass, field
from collections import defaultdict

from .config import ClickhouseSettings
from .table_structure import TableStructure, TableField


logger = getLogger(__name__)


CREATE_TABLE_QUERY = '''
CREATE TABLE {if_not_exists} `{db_name}`.`{table_name}`
(
{fields},
    `_version` UInt64,
    {indexes}
)
ENGINE = ReplacingMergeTree(_version)
{partition_by}ORDER BY {primary_key}
SETTINGS index_granularity = 8192
'''

DELETE_QUERY = '''
DELETE FROM `{db_name}`.`{table_name}` WHERE ({field_name}) IN ({field_values})
'''


@dataclass
class SingleStats:
    duration: float = 0.0
    events: int = 0
    records: int = 0

    def to_dict(self):
        return self.__dict__


@dataclass
class InsertEraseStats:
    inserts: SingleStats = field(default_factory=SingleStats)
    erases: SingleStats = field(default_factory=SingleStats)

    def to_dict(self):
        return {
            'inserts': self.inserts.to_dict(),
            'erases': self.erases.to_dict(),
        }


@dataclass
class GeneralStats:
    general: InsertEraseStats = field(default_factory=InsertEraseStats)
    table_stats: dict[str, InsertEraseStats] = field(default_factory=lambda: defaultdict(InsertEraseStats))

    def on_event(self, table_name: str, is_insert: bool, duration: float, records: int):
        targets = []
        if is_insert:
            targets.append(self.general.inserts)
            targets.append(self.table_stats[table_name].inserts)
        else:
            targets.append(self.general.erases)
            targets.append(self.table_stats[table_name].erases)

        for target in targets:
            target.duration += duration
            target.events += 1
            target.records += records

    def to_dict(self):
        results = {'total': self.general.to_dict()}
        for table_name, table_stats in self.table_stats.items():
            results[table_name] = table_stats.to_dict()
        return results


class ClickhouseApi:
    MAX_RETRIES = 5
    RETRY_INTERVAL = 30

    def __init__(self, database: str | None, clickhouse_settings: ClickhouseSettings):
        self.database = database
        self.clickhouse_settings = clickhouse_settings
        self.erase_batch_size = clickhouse_settings.erase_batch_size
        self.client = clickhouse_connect.get_client(
            host=clickhouse_settings.host,
            port=clickhouse_settings.port,
            username=clickhouse_settings.user,
            password=clickhouse_settings.password,
            connect_timeout=clickhouse_settings.connection_timeout,
            send_receive_timeout=clickhouse_settings.send_receive_timeout,
        )
        self.tables_last_record_version = {}  # table_name => last used row version
        self.stats = GeneralStats()
        self.execute_command('SET final = 1;')

    def get_stats(self):
        stats = self.stats.to_dict()
        self.stats = GeneralStats()
        return stats

    def get_tables(self):
        result = self.client.query('SHOW TABLES')
        tables = result.result_rows
        table_list = [row[0] for row in tables]
        return table_list

    def get_table_structure(self, table_name):
        return {}

    def get_databases(self):
        result = self.client.query('SHOW DATABASES')
        databases = result.result_rows
        database_list = [row[0] for row in databases]
        return database_list

    def execute_command(self, query):
        for attempt in range(ClickhouseApi.MAX_RETRIES):
            try:
                self.client.command(query)
                break
            except clickhouse_connect.driver.exceptions.OperationalError as e:
                logger.error(f'error executing command {query}: {e}', exc_info=e)
                if attempt == ClickhouseApi.MAX_RETRIES - 1:
                    raise e
                time.sleep(ClickhouseApi.RETRY_INTERVAL)

    def recreate_database(self):
        self.execute_command(f'DROP DATABASE IF EXISTS `{self.database}`')
        self.execute_command(f'CREATE DATABASE `{self.database}`')

    def get_last_used_version(self, table_name):
        return self.tables_last_record_version.get(table_name, 0)

    def set_last_used_version(self, table_name, last_used_version):
        self.tables_last_record_version[table_name] = last_used_version

    def create_table(self, structure: TableStructure, additional_indexes: list | None = None, additional_partition_bys: list | None = None):
        if not structure.primary_keys:
            raise Exception(f'missing primary key for {structure.table_name}')

        fields = [
            f'    `{field.name}` {field.field_type}' for field in structure.fields
        ]
        fields = ',\n'.join(fields)
        partition_by = ''

        # Check for custom partition_by first
        if additional_partition_bys:
            # Use the first custom partition_by if available
            partition_by = f'PARTITION BY {additional_partition_bys[0]}\n'
        else:
            # Fallback to default logic
            if len(structure.primary_keys) == 1:
                if 'int' in structure.fields[structure.primary_key_ids[0]].field_type.lower():
                    partition_by = f'PARTITION BY intDiv({structure.primary_keys[0]}, 4294967)\n'

        indexes = [
            'INDEX _version _version TYPE minmax GRANULARITY 1',
        ]
        if len(structure.primary_keys) == 1:
            indexes.append(
                f'INDEX idx_id {structure.primary_keys[0]} TYPE bloom_filter GRANULARITY 1',
            )
        if additional_indexes is not None:
            indexes += additional_indexes

        indexes = ',\n'.join(indexes)
        primary_key = ','.join(structure.primary_keys)
        if len(structure.primary_keys) > 1:
            primary_key = f'({primary_key})'

        query = CREATE_TABLE_QUERY.format(**{
            'if_not_exists': 'IF NOT EXISTS' if structure.if_not_exists else '',
            'db_name': self.database,
            'table_name': structure.table_name,
            'fields': fields,
            'primary_key': primary_key,
            'partition_by': partition_by,
            'indexes': indexes,
        })
        logger.debug(f'create table query: {query}')
        self.execute_command(query)

    def insert(self, table_name, records, table_structure=None):
        """Insert records into ClickHouse table"""
        if not records:
            return
            
        full_table_name = f'`{self.database}`.`{table_name}`'
        
        # Convert records to list if needed
        records_to_insert = list(records)
        
        logger.debug(f'Inserting into {full_table_name}')
        logger.debug(f'Records count: {len(records_to_insert)}')
        
        # Get table structure from ClickHouse
        try:
            # Use query to get table structure
            result = self.client.query(f"DESCRIBE {full_table_name}")
            column_names = [row['name'] for row in result.named_results()]
            column_types = [row['type'] for row in result.named_results()]
            logger.debug(f'ClickHouse table expects {len(column_names)} columns: {column_names}')
            logger.debug(f'ClickHouse table types: {column_types}')
        except Exception as e:
            logger.warning(f'Could not get table details: {e}')
        
        if records_to_insert:
            logger.debug(f'First record: {records_to_insert[0]}')
            logger.debug(f'First record length: {len(records_to_insert[0])}')
            
            # If it's a tuple, convert to list
            if isinstance(records_to_insert[0], tuple):
                records_to_insert = [list(record) for record in records_to_insert]
                logger.debug(f'Converted first record to list: {records_to_insert[0]}')
        
        try:
            self.client.insert(table=full_table_name, data=records_to_insert)
            logger.debug(f'Successfully inserted {len(records_to_insert)} records into {table_name}')
        except Exception as e:
            logger.error(f'Failed to insert into {table_name}: {e}')
            logger.error(f'Table: {full_table_name}')
            logger.error(f'Records count: {len(records_to_insert)}')
            if records_to_insert:
                logger.error(f'First record: {records_to_insert[0]}')
                logger.error(f'First record length: {len(records_to_insert[0])}')
            raise
        
    def _prepare_records_for_nullable(self, records, table_structure):
        """
        Prepare records for insertion by handling Nullable fields properly.
        For clickhouse-connect, we just pass None for NULL values.
        """
        prepared_records = []
        
        for record in records:
            prepared_record = []
            for i, field in enumerate(table_structure.fields):
                value = record[i] if i < len(record) else None
                
                # For Nullable fields, clickhouse-connect expects None for NULL values
                # and the actual value for non-NULL values
                prepared_record.append(value)
                    
            prepared_records.append(prepared_record)
        
        return prepared_records

    def erase(self, table_name, field_name, field_values):
        field_name = ','.join(field_name)
        
        # Batch large deletions to avoid ClickHouse max query size limit
        field_values_list = list(field_values)
        
        total_duration = 0.0
        total_records = len(field_values_list)
        
        for i in range(0, len(field_values_list), self.erase_batch_size):
            batch = field_values_list[i:i + self.erase_batch_size]
            batch_field_values = ', '.join(f'({v})' for v in batch)
            
            query = DELETE_QUERY.format(**{
                'db_name': self.database,
                'table_name': table_name,
                'field_name': field_name,
                'field_values': batch_field_values,
            })
            
            t1 = time.time()
            self.execute_command(query)
            t2 = time.time()
            total_duration += (t2 - t1)
        
        self.stats.on_event(
            table_name=table_name,
            duration=total_duration,
            is_insert=False,
            records=total_records,
        )

    def drop_database(self, db_name):
        self.execute_command(f'DROP DATABASE IF EXISTS `{db_name}`')

    def create_database(self, db_name):
        self.execute_command(f'CREATE DATABASE `{db_name}`')

    def select(self, table_name, where=None, final=None):
        query = f'SELECT * FROM {table_name}'
        if where:
            query += f' WHERE {where}'
        if final is not None:
            query += f' SETTINGS final = {int(final)};'
        result = self.client.query(query)
        rows = result.result_rows
        columns = result.column_names

        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        return results

    def query(self, query: str):
        return self.client.query(query)

    def show_create_table(self, table_name):
        return self.client.query(f'SHOW CREATE TABLE `{table_name}`').result_rows[0][0]

    def get_system_setting(self, name):
        results = self.select('system.settings', f"name = '{name}'")
        if not results:
            return None
        return results[0].get('value', None)

    def get_max_record_version(self, table_name):
        """
        Query the maximum _version value for a given table directly from ClickHouse.
        
        Args:
            table_name: The name of the table to query
            
        Returns:
            The maximum _version value as an integer, or None if the table doesn't exist
            or has no records
        """
        try:
            query = f"SELECT MAX(_version) FROM `{self.database}`.`{table_name}`"
            result = self.client.query(query)
            if not result.result_rows or result.result_rows[0][0] is None:
                logger.warning(f"No records with _version found in table {table_name}")
                return None
            return result.result_rows[0][0]
        except Exception as e:
            logger.error(f"Error querying max _version for table {table_name}: {e}")
            return None

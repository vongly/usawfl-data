import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    STATS_POSTGRES_USER,
    STATS_POSTGRES_PASSWORD,
    STATS_POSTGRES_HOST,
    STATS_POSTGRES_PORT, 
    STATS_POSTGRES_DB,
)


def connect_to_postgres(
    dbname,
    user,
    password,
    host,
    port,
):
    connection = psycopg.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        row_factory=dict_row,
    )

    return connection

class PostgresCall():
    def __init__(
        self,
        connection,
        test=False,
        test_limit=1,
    ):
    
        self.connection = connection
        self.test = test
        self.test_limit = test_limit

    def yield_records(self, schema, table, incremental_string=None, incremental_attribute=None):
        print('\n', '  ', f'{schema}.{table}')

        where_clause = f''' where {incremental_attribute} > '{incremental_string}' ''' if incremental_attribute and incremental_string else ''            

        if self.test:
            if not self.test_limit or self.test_limit < 1:
                self.test_limit = 1
        
        limit_clause = f' LIMIT {self.test_limit}' if self.test else ''

        query = f'select * from {schema}.{table} ' + where_clause + limit_clause + ';'


        with self.connection.cursor() as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()
            for row in rows:
                yield row


if __name__ == '__main__':
    conn = connect_to_postgres(
        dbname=STATS_POSTGRES_DB,
        user=STATS_POSTGRES_USER,
        password=STATS_POSTGRES_PASSWORD,
        host=STATS_POSTGRES_HOST,
        port=STATS_POSTGRES_PORT,
    )

    query_obj = PostgresQuery(connection=conn)
    results = query_obj.yield_records(
        schema='public',
        table='player_classifications_player',
        incremental_attribute='updated',
        incremental_string='2025-09-24 16:21:28.816974+00'
    )

from typing import Generator

from psycopg2.extras import execute_batch
from psycopg2.extensions import connection as _connection

from tools import (get_get_fields_on_schema_list,
                                      get_change_fields_txt,
                                      get_values_on_schema_test)


class PostgresSaver:
    """
        Вставка данных в Postgres
    """

    def __init__(self, pg_conn: _connection, size_rows: int):
        self.connection = pg_conn
        self.size_rows = size_rows

    def save_all_data(self, generator_ser_data: Generator, table_name: str):
        for batch in generator_ser_data:
            name_fields = get_change_fields_txt(batch[0])
            count_fields = len(get_get_fields_on_schema_list(batch[0]))
            count_values = ','.join(['%s'] * count_fields)
            values = get_values_on_schema_test(batch)

            with self.connection.cursor() as cursor:
                query = ('INSERT INTO content.{table_name} ({name_fields}) '
                         'VALUES ({count_values}) ON CONFLICT (id) DO NOTHING'
                         .format(table_name=table_name,
                                 name_fields=name_fields,
                                 count_values=count_values))
                execute_batch(cursor, query, values, page_size=self.size_rows)
                self.connection.commit()

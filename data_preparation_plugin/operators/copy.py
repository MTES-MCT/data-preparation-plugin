# -*- coding=utf-8 -*-

import textwrap
from airflow.operators.postgres_operator import PostgresOperator


stmt = textwrap.dedent("""
    DROP TABLE IF EXISTS {destination};
    CREATE TABLE {destination} (LIKE {source} including all);
    INSERT INTO {destination} SELECT * FROM {source}""")


class CopyTableOperator(PostgresOperator):

    def __init__(self, source, destination, **kwargs):
        sql = stmt.format(source=source, destination=destination)
        super().__init__(sql=sql, **kwargs)

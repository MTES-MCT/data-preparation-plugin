# -*- coding=utf-8 -*-

from unittest import TestCase, skip
import textwrap

from airflow.hooks.postgres_hook import PostgresHook
from airflow import settings
from airflow.models import Connection
from sqlalchemy import Column, String, Integer, BigInteger
import pandas as pd

from ..dataset import PostgresDataset


class DatasetTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        # create a connection to postgres
        conn = Connection(
            conn_id="postgres_test",
            conn_type="postgres",
            host="localhost",
            login="postgres",
            password="postgres",
            schema="postgres",
            port=5432
        )
        session = settings.Session()
        session.add(conn)
        session.commit()

        db = PostgresHook("postgres_test")
        create_schema = textwrap.dedent("""CREATE SCHEMA IF NOT EXISTS test""")
        db.run(create_schema)

    @classmethod
    def tearDownClass(cls):
        db = PostgresHook("postgres_test")
        drop_schema = textwrap.dedent("""DROP SCHEMA IF EXISTS test CASCADE""")
        db.run(drop_schema)

    def setUp(self):
        db = PostgresHook("postgres_test")

        create_table = textwrap.dedent("""
            CREATE TABLE IF NOT EXISTS test.test (
                id SERIAL PRIMARY KEY,
                foo VARCHAR(255)
            );
            """)
        db.run(create_table)
        self.dataset = PostgresDataset(
            name="test",
            schema="test",
            postgres_conn_id="postgres_test")

    def tearDown(self):
        db = PostgresHook("postgres_test")
        drop_table = textwrap.dedent("""
            DROP TABLE IF EXISTS test.test""")
        db.run(drop_table)

    def test_reflect(self):
        # it should return an ORM class
        Foo = self.dataset.reflect()
        self.assertEqual(Foo.__table__.name, "test")

    def test_reflect_no_primary_key(self):
        db = PostgresHook("postgres_test")

        # create a table with no primary key but with
        # some id which should be unique
        create_table = textwrap.dedent("""
            CREATE TABLE IF NOT EXISTS test.no_primary_key (
                some_id INTEGER,
                foo VARCHAR(255)
            );
            """)
        db.run(create_table, autocommit=True)
        dataset = PostgresDataset(
            name="no_primary_key",
            schema="test",
            postgres_conn_id="postgres_test")

        Foo = dataset.reflect(primary_key="some_id")
        self.assertEqual(Foo.__table__.name, "no_primary_key")

        drop_table = textwrap.dedent("""
            DROP TABLE IF EXISTS test.no_primary_key""")
        db.run(drop_table, autocommit=True)

    def test_writer(self):
        rows = [{"foo": "%s" % i} for i in range(0, 100)]
        with self.dataset.get_writer(chunksize=10) as writer:
            for row in rows:
                writer.write_row_dict(row)
        db = PostgresHook("postgres_test")
        count = db.get_first("SELECT COUNT(*) FROM test.test")[0]
        self.assertEqual(count, 100)

    def test_read_dtype(self):
        dtype = self.dataset.read_dtype()
        self.assertEqual(len(dtype), 2)
        foo_column = dtype[1]
        self.assertEqual(foo_column.name, 'foo')
        self.assertEqual(foo_column.table, None)

    def test_write_dtype(self):
        dtype = self.dataset.read_dtype()
        # add a column
        new_column = Column("new_column", String)
        dtype.append(new_column)
        self.dataset.write_dtype(dtype)
        new_dtype = self.dataset.read_dtype()
        self.assertEqual(len(new_dtype), 3)
        self.assertEqual(new_dtype[2].name, "new_column")

    def test_iter_rows(self):
        insert = textwrap.dedent("""
            INSERT INTO test.test (foo) VALUES ('foo1'), ('foo2')
        """)
        db = PostgresHook("postgres_test")
        db.run(insert)
        rows = []
        for row in self.dataset.iter_rows():
            rows.append(row)
        self.assertEqual(len(rows), 2)
        expected = [
            {'id': 1, 'foo': 'foo1'},
            {'id': 2, 'foo': 'foo2'}]
        self.assertEqual(rows, expected)

    def test_get_dataframe(self):
        insert = textwrap.dedent("""
            INSERT INTO test.test (foo) VALUES ('foo1'), ('foo2')
        """)
        db = PostgresHook("postgres_test")
        db.run(insert)
        df = self.dataset.get_dataframe()
        self.assertEqual(df.shape, (2, 2))
        self.assertEqual(list(df.columns), ["id", "foo"])

    def test_get_dataframes(self):
        insert = textwrap.dedent("""
            INSERT INTO test.test (foo) VALUES
            ('foo1'), ('foo2'), ('foo3'), ('foo4'), ('foo5')
        """)
        db = PostgresHook("postgres_test")
        db.run(insert)
        dataframes = self.dataset.get_dataframes(chunksize=2)
        self.assertEqual(len(list(dataframes)), 3)

    def test_write_with_schema(self):

        df = pd.DataFrame({
            "column1": [1, 2, 3, 4],
            "column2": ["a", "b", "c", "d"]
        })

        dataset = PostgresDataset(
            name="test_2",
            schema="test",
            postgres_conn_id="postgres_test")

        dataset.write_with_schema(df)

        dtype = dataset.read_dtype(primary_key="id")

        column_names = [column.name for column in dtype]

        expected = ["id", "column1", "column2"]

        self.assertEqual(column_names, expected)

    def test_write_dataframe(self):

        df1 = pd.DataFrame({
            "foo": ["foo1", "foo2"]
        })

        df2 = pd.DataFrame({
            "foo": ["foo3", "foo4"]
        })

        with self.dataset.get_writer() as writer:
            writer.write_dataframe(df1)
            writer.write_dataframe(df2)

        db = PostgresHook("postgres_test")
        count = db.get_first("SELECT COUNT(*) FROM test.test")[0]

        records = db.get_records("SELECT * from test.test")

        self.assertEqual(count, 4)

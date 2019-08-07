# -*- coding=utf-8 -*-

from unittest import TestCase, skip
import textwrap

from airflow.hooks.postgres_hook import PostgresHook
from airflow import settings
from airflow.models import Connection
from sqlalchemy import Column, String, Integer

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
        rows = [{"foo": "bar1"}, {"foo": "bar2"}]
        with self.dataset.get_writer() as writer:
            for row in rows:
                writer.write_row_dict(row)
        db = PostgresHook("postgres_test")
        cur = db.get_cursor()
        cur.execute("SELECT COUNT(*) FROM test.test")
        count = cur.fetchone()[0]
        self.assertEqual(count, 2)

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

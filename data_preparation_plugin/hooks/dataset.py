# -*- coding=utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table

Base = declarative_base()


class PostgresDatasetWriter(object):
    """
    Provides a writer to PostgresDataset
    """

    def __init__(self, dataset):
        self.dataset = dataset

    def __enter__(self):
        self.rows = []
        return self

    def __exit__(self, *args):
        Model = self.dataset.reflect()
        session = self.dataset.get_session()
        session.bulk_insert_mappings(Model, self.rows)
        session.commit()

    def write_row_dict(self, row):
        self.rows.append(row)


class PostgresDataset(PostgresHook):
    """
    Represents an interface to a specific Postgres table

    Example usage

    dataset = PostgresDataset(
        postgres_conn_id='postgres_kelrisks',
        schema='etl',
        name='basol')

    df = dataset.get_dataframe()

    :param pg_schema: name of the PostgreSQL schema the dataset belongs to
    :param name: name of the PostgreSQL table
    :param create_stmt: name of the file
    """

    def __init__(self, name, schema=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_schema = schema
        self.name = name
        self.engine = self.get_sqlalchemy_engine()
        self.mapper = None

    def reflect(self, force=False):
        """
        This function allow to reflect the schema of an existing
        table, returning an sqlalchemy ORM mapped class to work with
        """
        def _reflect():
            class Mapper(Base):
                __table__ = Table(
                    self.name,
                    Base.metadata,
                    schema=self.pg_schema,
                    autoload=True,
                    autoload_with=self.engine)
            return Mapper

        if force:
            self.mapper = None
        self.mapper = self.mapper or _reflect()
        return self.mapper

    def get_dataframe(self, sql=None):
        """ Read the whole dataset into a pandas dataframe """
        if sql is None:
            sql = "SELECT * from {self.pg_schema}.{self.name}" \
                .format(
                    pg_schemaname=self.pg_schema,
                    name=self.name)
        return self.get_pandas_df(sql)

    def iter_rows(self):
        raise NotImplemented()

    def get_writer(self):
        """ Returns a writer to the dataset using a context manager """
        return PostgresDatasetWriter(self)

    def read_dtype(self):
        """
        Returns the data type of the dataset as a list
        of SQL Alchemy colums
        """
        Mapper = self.reflect()
        columns = Mapper.__table__.columns
        return [column.copy() for column in columns]

    def write_dtype(self, dtype):
        """
        Write a new datatype, dropping the table if it exists
        """
        extend_existing = False
        if self.mapper:
            extend_existing = True
        table = Table(
            self.name,
            Base.metadata,
            schema=self.pg_schema,
            extend_existing=extend_existing,
            *dtype)
        table.drop(self.engine, checkfirst=True)
        table.create(self.engine)
        self.reflect(force=True)

    def get_session(self):
        Session = sessionmaker()
        return Session(bind=self.engine)

    # TODO delete
    def write_from_dataframe(self, df, dtype=None):
        """
        Write the dataframe to the dataset, overwriting
        previous data. The schema of the dataframe must match
        the schema of the dataset
        """

        engine_kwargs = {"use_batch_mode": True}
        engine = self.get_sqlalchemy_engine(engine_kwargs)

        # TODO this is slow
        # Check theis solution
        # https://www.codementor.io/bruce3557/
        # graceful-data-ingestion-with-sqlalchemy-and-pandas-pft7ddcy6
        df.to_sql(
            self.name,
            engine,
            schema=self.pg_schema,
            if_exists='replace',
            index=True,
            dtype=dtype)

    # TODO delete
    def left_join(self, dataset, join_key_a, join_key_b):
        """
        Perform an SQL join with another dataset
        """
        fmt_params = {
            "table_name": self.table_name,
            "join_with": dataset.table_name,
            "join_key_a": join_key_a,
            "join_key_b": join_key_b
        }
        sql = textwrap.dedent("""
            SELECT * from {table_name} as a
            LEFT JOIN {join_with} as b
            ON a.{join_key_a} = b.{join_key_b}
            """.format(**fmt_params))
        df = self.get_pandas_df(sql)
        dtype = {**self.dtype, **dataset.dtype}

        return (df, dtype)

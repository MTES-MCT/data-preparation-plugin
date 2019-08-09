# -*- coding=utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table

Base = declarative_base()


class PostgresDatasetWriter(LoggingMixin):
    """
    Provides a writer to PostgresDataset
    """

    def __init__(self, dataset, chunksize=1000):
        self.dataset = dataset
        self.chunksize = chunksize

    def __enter__(self):
        self.rows = []
        self.inserted = 0
        return self

    def __exit__(self, *args):
        self.flush()

    def flush(self):
        """ insert data into table """
        if self.rows:
            Mapper = self.dataset.reflect()
            engine = self.dataset.engine
            engine.execute(Mapper.__table__.insert(), self.rows)
            self.rows = []
            msg = "Inserted {n} rows into table {schema}.{name}".format(
                n=self.inserted,
                schema=self.dataset.pg_schema,
                name=self.dataset.name)
            self.log.info(msg)

    def write_row_dict(self, row):
        self.rows.append(row)
        self.inserted += 1
        if self.inserted % self.chunksize == 0:
            self.flush()

    def write_dataframe(self, df, dtype=None):
        """
        Write the dataframe to the dataset, appending data
        The schema of the dataframe must match the schema of the dataset
        """
        df.to_sql(
            self.dataset.name,
            self.dataset.engine,
            schema=self.dataset.pg_schema,
            index=False,
            if_exists='append')


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
        engine_kwargs = {"use_batch_mode": True}
        self.engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        self.mapper = None

    def reflect(self, force=False, primary_key=None):
        """
        This function allow to reflect the schema of an existing
        table, returning an sqlalchemy ORM mapped class to work with
        """
        def _reflect():

            table = Table(
                self.name,
                Base.metadata,
                schema=self.pg_schema,
                autoload=True,
                autoload_with=self.engine)

            class Mapper(Base):
                __table__ = table

                # See https://docs.sqlalchemy.org/en/13/faq/ormconfiguration.html
                # #how-do-i-map-a-table-that-has-no-primary-key
                __mapper_args__ = {
                    'primary_key': [getattr(table.c, primary_key)]
                } if primary_key else {}

            return Mapper

        if force:
            self.mapper = None

        self.mapper = self.mapper or _reflect()

        return self.mapper

    def get_dataframe(self):
        """ Read the whole dataset into a pandas dataframe """
        sql = "SELECT * from {pg_schema}.{name}" \
            .format(
                pg_schema=self.pg_schema,
                name=self.name)
        return self.get_pandas_df(sql)

    def get_dataframes(self, chunksize):
        """ Read the whole dataset as chunked pandas dataframes """
        sql = "SELECT * from {pg_schema}.{name}" \
            .format(
                pg_schema=self.pg_schema,
                name=self.name)

        import pandas.io.sql as psql

        return psql.read_sql(sql, con=self.engine, chunksize=chunksize)

    def iter_rows(self, head=None):
        """
        Returns an iterator over all the rows of the datatset
        represented as dictionnary of (column, value) pairs
        """
        def row2dict(row):
            d = {}
            for column in row.__table__.columns:
                d[column.name] = getattr(row, column.name)
            return d

        Mapper = self.reflect()
        session = self.get_session()
        q = session.query(Mapper)
        if head:
            q = q.limit(head)
        rows = [row2dict(r) for r in q.all()]
        session.close()
        return rows

    def get_writer(self, *args, **kwargs):
        """ Returns a writer to the dataset using a context manager """
        return PostgresDatasetWriter(self, *args, **kwargs)

    def read_dtype(self, **kwargs):
        """
        Returns the data type of the dataset as a list
        of SQL Alchemy colums
        """
        Mapper = self.reflect(**kwargs)
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

    def write_with_schema(self, df):
        """ Write a pandas dataframe to the datatet
        using implicit conversions between pandas types
        and PostgreSQL types
        """
        df.to_sql(
            self.name,
            self.engine,
            schema=self.pg_schema,
            index=True,
            index_label="id",
            if_exists="replace",
            method="multi")

    def get_session(self):
        Session = sessionmaker()
        return Session(bind=self.engine)

# -*- coding=utf-8 -*-

from airflow.plugins_manager import AirflowPlugin
from .hooks.dataset import PostgresDataset
from .operators.download import DownloadUnzipOperator
from .operators.embulk import EmbulkOperator
from .operators.shp2pgsql import Shp2pgsqlOperator
from .operators.copy import CopyTableOperator


class DataPreparationPlugin(AirflowPlugin):
    name = "data_preparation"
    hooks = [PostgresDataset]
    operators = [
        DownloadUnzipOperator,
        EmbulkOperator,
        Shp2pgsqlOperator,
        CopyTableOperator]
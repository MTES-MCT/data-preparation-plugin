# -*- coding=utf-8 -*-

from airflow.plugins_manager import AirflowPlugin
from .hooks.dataset import PostgresDataset
from .operators.download import DownloadUnzipOperator
from .operators.embulk import EmbulkOperator


class DataPreparationPlugin(AirflowPlugin):
    name = "data_preparation"
    hooks = [PostgresDataset]
    operators = [
        DownloadUnzipOperator,
        EmbulkOperator]
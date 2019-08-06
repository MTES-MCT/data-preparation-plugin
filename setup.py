from setuptools import setup

setup(
    name="data_preparation_plugin",
    version='1.0',
    entry_points={
        'airflow.plugins': [
            'data_preparation_plugin = data_preparation_plugin.data_preparation_plugin:DataPreparationPlugin'
        ]
    }
)
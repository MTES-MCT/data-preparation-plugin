from setuptools import setup, find_packages

setup(
    name="data_preparation_plugin",
    version='0.1.7',
    description="Airflow plugin with database hooks and" +
                "operators for common data preparation tasks",
    entry_points={
        "airflow.plugins": [
            "data_preparation_plugin = " +
            "data_preparation_plugin.data_preparation_plugin:" +
            "DataPreparationPlugin"
        ]
    },
    author_email="benoit.guigal@protonmail.com",
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),  # Required
    python_requires=">=3.6"
)

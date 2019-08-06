# data-preparation-plugin
Airflow plugin with custom hooks and operators for Kelrisks and Trackdéchets ETL


# Run the tests

Create a virtualenv and install dependencies

```
pip install -r requirements.txt
```

Initialize Airflow metastore

```
airflow initdb
```

Launch a local PostgreSQL instance

```
docker run -p 5432:5432 postgres:9.6
```

Run the tests

```
python -m unittest
```


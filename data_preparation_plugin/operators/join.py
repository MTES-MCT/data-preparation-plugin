# -*- coding=utf-8 -*-

from airflow.operators.python_operator import PythonOperator


class JoinOperator(PythonOperator):
    """
    Perform an SQL join between two datasets and write
    the result to an output dataset
    """
    def __init__(self, dataset, join_with, join_key_a, join_key_b,
                 output_dataset, *args, **kwargs):
        self.dataset = dataset
        self.join_with = join_with
        self.join_key_a = join_key_a
        self.join_key_b = join_key_b
        self.output_dataset = output_dataset
        super().__init__(python_callable=self.join, *args, **kwargs)

    def join(self):
        (df, dtype) = self.dataset.left_join(self.join_with, self.join_key_a,
                                             self.join_key_b)
        self.output_dataset.write_from_dataframe(df, dtype)

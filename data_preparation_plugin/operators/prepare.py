# -*- coding=utf-8 -*-

from airflow.operators.python_operator import PythonOperator


class PrepareDatasetOperator(PythonOperator):
    """
    PrepareDatasetOperator applies a series of pandas processors
    to a dataset and output another one
    """

    def __init__(self, input_dataset, output_dataset,
                 processors, *args, **kwargs):
        python_callable = self.transform_load
        self.input_dataset = input_dataset
        self.output_dataset = output_dataset
        self.processors = processors
        super().__init__(python_callable=python_callable, *args, **kwargs)

    def transform_load(self):
        df = self.input_dataset.get_dataframe()
        dtype = self.input_dataset.dtype
        for (processor, kwargs) in self.processors:
            (df, dtype) = processor(df, dtype, **kwargs)
        self.output_dataset.write_from_dataframe(df, dtype)
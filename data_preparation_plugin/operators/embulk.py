# -*- coding=utf-8 -*-

import os

from airflow.operators.bash_operator import BashOperator


class EmbulkOperator(BashOperator):
    """
    Embulk is an open source data loader that helps data transfer
    between various databases. https://www.embulk.org/
    This operater calls the embulk binary on a specific
    embulk config file located in $EMBULK_DIR.
    We mainly use it to load data from a csv file to a Postgres table
    """

    def __init__(self, embulk_config, **kwargs):
        env = {
            **kwargs.pop("env", {}),
            **os.environ.copy(),
            'EMBULK_CONFIG': embulk_config
        }
        cmd = '$EMBULK_BIN run $EMBULK_DIR/$EMBULK_CONFIG'
        super().__init__(bash_command=cmd, env=env, **kwargs)
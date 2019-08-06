# -*- coding=utf-8 -*-
import os
import textwrap

from airflow.operators.bash_operator import BashOperator


class DownloadUnzipOperator(BashOperator):
    """
    DownloadUnzipOperator download a zip file on a specific
    url and extract the content to the data directory
    """

    def __init__(self, url, **kwargs):
        env = {**os.environ.copy(), "URL": url}
        bash_command = textwrap.dedent("""
            TMPFILE=`mktemp`
            PWD=`pwd`
            wget "$URL" -O $TMPFILE
            mkdir -p $ROOT_DIR/data
            unzip -o -d $ROOT_DIR/data $TMPFILE""")
        super().__init__(bash_command=bash_command, env=env, **kwargs)
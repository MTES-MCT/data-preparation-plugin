# -*- coding=utf-8 -*-
import os
import textwrap

from airflow.operators.bash_operator import BashOperator


class DownloadUnzipOperator(BashOperator):
    """
    DownloadUnzipOperator download a zip file on a specific
    url and extract the content to the data directory
    """

    def __init__(self, url, dir_path, **kwargs):
        env = {**os.environ.copy(), "URL": url, "DIR_PATH": dir_path}
        bash_command = textwrap.dedent("""
            TMPFILE=`mktemp`
            PWD=`pwd`
            wget "$URL" -O $TMPFILE
            mkdir -p $DIR_PATH
            unzip -o -d $DIR_PATH $TMPFILE""")
        super().__init__(bash_command=bash_command, env=env, **kwargs)
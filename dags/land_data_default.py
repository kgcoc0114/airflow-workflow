# -*- coding: utf-8 -*-
import os
import sys
from builtins import range
from datetime import timedelta, datetime

from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

_SSH = 'hadoop_ssh'
_PROJECT = 'lvr-land-crawler-spark'
parent_dir = "/".join(os.path.abspath(__file__).split("/")[:-2])
sys.path.append(parent_dir)
import tools.slack_api as slack

args = {
    'owner': 'Farah.C',
    'start_date': datetime(2020, 6, 25),
}

dag_land_default = DAG(
    dag_id = '{}_default'.format(_PROJECT),
    default_args = args,
    schedule_interval = '5 0 2,12,22 * *',
    dagrun_timeout = timedelta(minutes=60),
    tags = ['Crawler', 'Spark']
)

pull_project = SSHOperator(
    ssh_conn_id = "{}".format(_SSH),
    task_id = 'pull_project',
    command = 'sh ~/{}/bashs/pull_project.sh '.format(_PROJECT),
    on_failure_callback = slack.slack_failed_task,
    dag = dag_land_default)

denpendency_exist = SSHOperator(
    ssh_conn_id = "{}".format(_SSH),
    task_id = 'dependency_exist_or_not',
    command = 'sh ~/{}/bashs/dependency_exist_or_not.sh '.format(_PROJECT),
    on_failure_callback = slack.slack_failed_task,
    dag = dag_land_default)

get_land_data = SSHOperator(
    ssh_conn_id = "{}".format(_SSH),
    task_id = 'get_land_data',
    command = 'sh ~/{}/bashs/run_crawler.sh '.format(_PROJECT),
    on_failure_callback = slack.slack_failed_task,
    dag = dag_land_default)

run_spark = SSHOperator(
    ssh_conn_id = "{}".format(_SSH),
    task_id = 'run_spark',
    command = 'sh ~/{}/bashs/run_spark.sh '.format(_PROJECT),
    on_failure_callback = slack.slack_failed_task,
    dag = dag_land_default)

check_output = SSHOperator(
    ssh_conn_id = "{}".format(_SSH),
    task_id = 'check_output',
    command = 'sh ~/{}/bashs/check_result.sh '.format(_PROJECT),
    on_success_callback = slack.slack_success_task,
    on_failure_callback = slack.slack_failed_task,
    dag = dag_land_default)

# workflow
pull_project >> denpendency_exist >> get_land_data >> run_spark >> check_output
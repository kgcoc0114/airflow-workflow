# -*- coding: utf-8 -*-
import os
import configparser
from datetime import timedelta, datetime
from airflow.operators.slack_operator import SlackAPIPostOperator
parent_dir = "/".join(os.path.abspath(__file__).split("/")[:-2])

# get slack config
config = configparser.ConfigParser()
config.read('{}/config/lvr_land_config.ini'.format(parent_dir))
_slack_channel = config.get('Slack', 'channel_id')#GET "Value_ABC"
_slack_token = config.get('Slack', 'token') #Get "Some thing here"

def slack_failed_task(contextDictionary, **kwargs):
    failed_alert = SlackAPIPostOperator(
        task_id = 'slack_failed',
        channel = "#{}".format(_slack_channel),
        token = "{}".format(_slack_token),
        text =
        """:rain_cloud: [ DAG : {} - Task : {} ]\nFailed on : {}\n------------- Exception -------------\n{}""".format(
            contextDictionary["dag"].dag_id,
            contextDictionary["task"].task_id,
            datetime.utcnow().isoformat()+"Z",
            contextDictionary["exception"]),
        owner = 'Farah.C',)
    return failed_alert.execute()


def slack_success_task(contextDictionary, **kwargs):
    success_alert = SlackAPIPostOperator(
        task_id='slack_success',
        channel="#{}".format(_slack_channel),
        token= "{}".format(_slack_token),
        text =
        """:sunny: [ DAG : {} ]\nSucceeded on: {}
        """.format(contextDictionary["dag"].dag_id,
            datetime.utcnow().isoformat()+"Z"),
        owner = 'Farah.C',)
    return success_alert.execute()

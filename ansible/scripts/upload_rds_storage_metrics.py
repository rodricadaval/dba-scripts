#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform replace table to new one reducing archiving old rows and converting columns to biginteger.

import os
import requests
import json
from base64 import b64encode
import boto3
import datetime
import psycopg2
import re
from pprint import pprint
from datetime import datetime, timedelta
import sys
import argparse
import time
import shutil
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
from lib import dynamo_rds
from lib.settings import Settings as global_settings
import logging
from botocore.exceptions import ClientError
from botocore.config import Config
import traceback

logging.basicConfig(level=logging.INFO)
FORMATTER = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

my_logger = logging.getLogger()

parser = argparse.ArgumentParser(description='Killing DDL dba queries before affecting production')

parser.add_argument('-i', action="store", dest="db_instance", help="Rds DBInstanceIdentifier",
                    required=True)
parser.add_argument('-p', '--profile', action="store", dest="aws_profile", default="xx-xx-west-2",
                    help="AWS Account profiles", required=True)
parser.add_argument('-e', '--engine', action="store", dest="dbengine", default="all", help="postgres, mysql, aurora, aurora-mysql, aurora-postgres", required=False)

args = parser.parse_args()

db_instance = args.db_instance
fullprofile = args.aws_profile
boto3.setup_default_session(profile_name=fullprofile)
awsengine = args.dbengine

slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXX"
slackChannel = "dba_boto3_reports"

def sendToSlack(var_data):
    """
    Slack
    """
    color = "#FF164f"
    thumb = "https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", \
            "author_name": "Instance with max transaction ids reaching limit. Please perform vacuumdb to tables in conflict: '+str(db_instance)+' ", \
            "fields": [ \
            {"title": "Profile","value": "' + str(fullprofile) + '","short": true}, \
            {"title": "Host","value": "'+str(db_instance)+'","short": true}, \
            {"title": "Max","value": "' + str(var_data[0]) + '","short": true}, \
            {"title": "Actual","value": "' + str(var_data[1]) + '","short": true}, \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "last check", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-RDS-TXIDs-LIMIT", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


def __get_rds_metrics():
    queries = { 'metrics':
        [{
            'Id': 'request1',
            'Label': "MaximumUsedTransactionIDs",
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/RDS',
                    'MetricName': 'MaximumUsedTransactionIDs',
                    'Dimensions': [
                        {
                            'Name': 'DBInstanceIdentifier',
                            'Value': db_instance
                        }
                    ]
                },
                'Period': 60,
                'Stat': 'Average'
            }
        }]
    }

    api_config = Config(
        region_name = fullprofile.split("-",1)[1],
        connect_timeout = 5,
        read_timeout = 60,
        retries = {
            'max_attempts': 50,
            'mode': 'standard'
        }
    )
    session = boto3.session.Session(profile_name=fullprofile)
    cloudwatch = session.client('cloudwatch', config=api_config)

    try:
        # get metrics from the last 5 minutes
        response = cloudwatch.get_metric_data(
            MetricDataQueries = queries['metrics'],
            StartTime = (datetime.now() - timedelta(seconds=60 * 1)).timestamp(),
            EndTime = datetime.now().timestamp(),
        )

        if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
            if len(response['MetricDataResults'][0]['Values']) > 0:
                max_used_transaction_ids = round(response['MetricDataResults'][0]['Values'][0], 2)
            else:
                max_used_transaction_ids = -1
            return {
                'MaximumUsedTransactionIDs': max_used_transaction_ids
            }
        else:
            return {'MaximumUsedTransactionIDs': -1}
    except Exception as e:
        my_logger.error("There was an error trying to read cloudwatch metrics. Check if the endpoint is in the "
                          "correct aws profile and try again")
        my_logger.error("Error: {}".format(e))
        my_logger.warning("Finished")
        sys.exit(5)


def scan_parameter(parameter, throttle=0.2):
    try:
        rdsclient = boto3.client('rds')
        response = rdsclient.describe_db_parameters(
            DBParameterGroupName=parameter,
            MaxRecords=20,
            Marker='string'
        )
        for each_parameter in response["Parameters"]:
            if each_parameter["ParameterName"] == 'autovacuum_freeze_max_age':
                cloudwatch_metric = __get_rds_metrics()
                if "ParameterValue" in each_parameter.keys():
                    parameter_value = round(int(each_parameter["ParameterValue"]), 0)
                else:
                    parameter_value = 200000000
                if parameter_value <= round(int(cloudwatch_metric['MaximumUsedTransactionIDs']),0) * 1.05:
                    sendToSlack([parameter_value, round(int(cloudwatch_metric['MaximumUsedTransactionIDs']),0)])
    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return scan_parameter(parameter, throttle*2)
        else:
            my_logger.error("Error getting aws parameter values:" + str(e))
            my_logger.error(traceback.format_exc())
            sys.exit(1)

def process_instance(rds_instances):
    my_logger.info(rds_instances["DBInstances"])
    for i in rds_instances["DBInstances"]:
        parameter_group = i["DBParameterGroups"][0]["DBParameterGroupName"]
    #scan_parameter(parameter_group)

def main():
    """
    Describe DBInstance
    """
    rdsclient = boto3.client('rds')
    paginator = rdsclient.get_paginator('describe_db_instances')
    filter_list = []
    if awsengine not in ["docdb", "all"]:
        filter_list.append({'Name': 'engine', 'Values': [awsengine]})
    if "db_instance" in globals():
        pages = paginator.paginate(DBInstanceIdentifier=db_instance, Filters=filter_list)
    else:
        pages = paginator.paginate(Filters=filter_list)

    for page in pages:
        process_instance(page)

    print()
    return my_logger.info("Finished")

if __name__ == '__main__':
    main()

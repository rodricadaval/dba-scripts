#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#

import os
import requests
import json
from base64 import b64encode
import boto3
import datetime
import subprocess
import psycopg2
import re
from pprint import pprint
import sys
import argparse
import shelve
import time
import shutil
import logging
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
sys.path.insert(0, git_root + '/mongodb/common')
import gmail

parser = argparse.ArgumentParser(description='Check RDS Backup Retention IS OK')

parser.add_argument('-p', '--profile', action="store", dest="awsprofile", default="xx-west-2",
                    help="Name of the aws credential profile")
parser.add_argument('-e', '--engine', action="store", dest="dbengine", default="", help="postgres, mysql or docdb")

args = parser.parse_args()

rdsengine = args.dbengine
global fullData
fullData = {}
fullprofile = args.awsprofile
fullData[fullprofile] = {}
pprint("Profile: " + fullprofile)
boto3.setup_default_session(profile_name=fullprofile)
min_retention={"aurora": 6, "docdb": 1, "mysql": 6, "neptune": 1, "postgres": 6, "aurora-mysql": 6, "default": 1}
slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
slackChannel = "test_channel"

def send_to_slack(var_data):
    """
    Slack
    """
    color = "#FF164f"
    thumb = "https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", \
            "author_name": "RDS with bad backup retention setting", \
            "fields": [ \
            {"title": "RDS", "value": "' + str(var_data[0]) + '", "short": true}, \
            {"title": "Actual Retention", "value": "' + str(var_data[1]) + '","short": true}, \
            {"title": "Expected Retention", "value": "' + str(min_retention[str(var_data[2])]) + '","short": true}, \
            {"title": "Engine","value": "' + str(var_data[2]) + '","short": true}, \
            {"title": "AWS Account","value": "' + str(fullprofile) + '","short": true}, \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "time", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-RDS-bad-setting", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)

def format_seq_check(data):
    try:
        send_to_slack(data)

        message = "Instance: " + str(data[0]) + "\n " \
                  "\tBackupRetentionPeriod: " + str(data[1]) + "\n " \
                  "\tExpected: " + str(min_retention[str(data[2])]) + "\n " \
                  "\tEngine: " + str(data[2]) + "\n\n"

        return message

    except Exception as e:
        pprint("Error:" + str(e))
        sys.exit(1)

def custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(screen_handler)

    return logger

def get_cluster_members(engine, cluster_name):
    rdsclient = boto3.client('rds')
    cluster_data = rdsclient.describe_db_clusters(
        DBClusterIdentifier=cluster_name,
        Filters=[{'Name': 'engine', 'Values': [engine]}],
        MaxRecords=20
    )
    return cluster_data["DBClusters"][0]["DBClusterMembers"]

def is_master(i):
    if i["Engine"] in ["docdb", "aurora", "aurora-mysql", "aurora-postgres"]:
        for docdb_i in get_cluster_members(i["Engine"], i["DBClusterIdentifier"]):
            if docdb_i["DBInstanceIdentifier"] == i["DBInstanceIdentifier"] and docdb_i["IsClusterWriter"]:
                return True
        return False
    elif i["Engine"] in ["postgres", "mysql"]:
        return "ReadReplicaSourceDBInstanceIdentifier" not in i.keys()

def process_instances(rdsInstances):
    """
    Recorro las instancias.
    Luego, guardo resultados negativos.
    """
    for i in rdsInstances["DBInstances"]:
        if is_master(i):
            if i["Engine"] in min_retention.keys():
                min_val = min_retention[i["Engine"]]
            else:
                min_val = min_retention["default"]
            if int(i["BackupRetentionPeriod"]) < min_val:
                if i["Engine"] not in fullData[fullprofile].keys():
                    fullData[fullprofile][i["Engine"]] = {}
                fullData[fullprofile][i["Engine"]][i["DBInstanceIdentifier"]] = i["BackupRetentionPeriod"]


def send_if_any(res):
    message = ""
    each = [None] * 3
    if res:
        for engine in res[fullprofile].keys():
            for instance in res[fullprofile][engine]:
                each[0] = instance
                each[1] = str(res[fullprofile][engine][instance])
                each[2] = engine
                message += format_seq_check(each)

    if message:
        body = "Please review this instance settings. There are wrong backup retentions \n\n"
        sender = "rodri.cadaval@gmail.com"
        to = "rodri.cadaval@gmail.com"
        body += message
        subject = str(fullprofile).upper() + '- RDS Instances with bad setting -> Backup Retention Period'
        gm = gmail.Gmail(sender)
        gm.send_message(to, subject, body)


def check_rds_backup_retention():
    """
  Get list of all the DBs from RDS
  """
    try:
        rdsclient = boto3.client('rds')
        paginator = rdsclient.get_paginator('describe_db_instances')
        if rdsengine:
            filter = {'Name': 'engine', 'Values': [rdsengine]}
            pages = paginator.paginate(Filters=[filter])
        else:
            pages = paginator.paginate()

        for page in pages:
            process_instances(page)

        send_if_any(fullData)
    except Exception as e:
        logger.error("AWS API error: %s", e)
        sys.exit(501)


def main():
    global logger
    check_rds_backup_retention()


if __name__ == '__main__':
    logger = custom_logger(os.path.basename(__file__))
    main()

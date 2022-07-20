#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform custom queries in a groups or an individual RDS

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
import argparse
import shelve
import time
import shutil
import traceback
import sys

git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
sys.path.insert(0, git_root + '/mongodb/common')
from lib import opsgenie
from lib import dynamo
import logging
import getpass
import gmail

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.basicConfig(level=logging.INFO)


def valid_profile(s):
    try:
        if s in ["xx-xx-east-1", "xx-xx-west-2", "xx1-xx-east-1", "xx1-xx-west-2", "xxx-xxx-east-1",
                        "xxx-xxx-west-2", "xx1-xx-east-1", "xxxx-xx-west-2"]:
            return s
        else:
            raise ValueError(s)
    except ValueError:
        msg = "Not a valid profile: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser(description='Showing Custom Queries Results')

parser.add_argument('--profile', action="store", dest="profile", help="xx-xx-east-1 | xx-xx-west-2 | xx1-xx-east-1 | "
                                                                      "xx1-xx-west-2 | xxx-xxx-east-1 | xxx-xxx-west-2 | "
                                                                      "xx1-xx-east-1 | xxxx-xx-west-2",
                    required=True, type=valid_profile)
parser.add_argument('-u', '--user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication")
parser.add_argument('-p', '--password', action="store", dest="secret", help="Database Name",
                    required=False)
parser.add_argument('-r', '--rds-instance', action="store", dest="instance", default="postgres", help="Engine",
                    required=False)
parser.add_argument('-e', '--engine', action="store", dest="engine", default="postgres", help="Engine",
                    required=False)
parser.add_argument('-f', '--file', action="store", dest="file", help="File path",
                    required=True)
parser.add_argument('-l', '--list-users', action="store", dest="receivers", default="",
                    help="User list separated by comma", required=False)

args = parser.parse_args()

fullprofile = args.profile
rds_engine = args.engine
user = args.rep_user
rds_instance = args.instance
file = args.file
filename = os.path.basename(file)
if args.receivers:
    arg_user_list = str(args.receivers).lower()
    list_users = [x.strip() for x in arg_user_list.split(',')]
if args.secret is not None:
    secret = args.secret

boto3.setup_default_session(profile_name=fullprofile)

slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXX"
slackChannel = "dba_boto3_reports"


def send_to_slack(var_data):
    """
    Slack
    """
    color = "#FF164f"
    thumb = "https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"

    fields_list = []
    for key in var_data:
        if key != "endpoint":
            fields_list.append({"title": str(key).capitalize(), "value": str(var_data[key]).replace('"', ''), "short": "true"})
        else:
            fields_list.append({"title": str(key).capitalize(), "value": str(var_data[key]).replace('"', ''), "short": "false"})

    slack_data = {
        "username": "DBA-Boto3Bot",
        "icon_emoji": ":satellite:",
        "channel": "#"+slackChannel,
        "attachments": [
            {
                "color": "#FF564f",
                "pretext": "Custom Query Alert",
                "title": filename,
                "thumb_url": thumb,
                "author_name": "@rodrigo.cadaval",
                "author_link": "https://bitbucket.org/X1Xnc/dba-scripts/src/master/",
                "author_icon": "https://emoji.slack-edge.com/T2QSQ3L48/rodrigocadaval/d1a284d0b8e3682e.jpg",
                "fields": fields_list
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}

    try:
        response = requests.post(slackUrl, data=json.dumps(slack_data), headers=headers)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


def format_check(data):
    try:
        message = ""
        send_to_slack(data)
        message = "<li><b>" + data['database'] + "</b><ul>"
        for each_key in data:
            if each_key != "database":
                message = message + "<li> "+each_key+": " + str(data[each_key]) + "</li>"
        message = message + "</ul></li>"
        return message

    except Exception as e:
        pprint("Error:" + str(e))
        sys.exit(1)


def return_correct_country(profile):
    if profile == "xx1-xx-east-1":
        return "m"
    elif profile == "xx-xx-east-1":
        return "a"
    elif profile == "xxx-xxx-east-1":
        return "dev"
    elif profile == "xxxx-xx-west-2":
        return "xx1"
    elif profile == "xx-xx-west-2":
        return "c"
    else:
        return "c"


def check_secret():
    global secret
    if user == "dba_test_service":
        i_dynamo = dynamo.DynamoClient(return_correct_country(fullprofile))
        var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
        secret = var['value']


def open_conn(db, instance_data):
    global secret
    try:
        conn = psycopg2.connect(
            "dbname='{}' host='{}' user='{}' password='{}' port={} connect_timeout=5"
                .format(db.replace("\"", ""), instance_data['Endpoint']['Address'], user, secret,
                        instance_data['Endpoint']['Port']))
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        if "password authentication failed for user" in str(e):
            logging.info("Check login role credentials for host {} and user {}. Error: {}"
                         .format(instance_data['Endpoint']['Address'], user, e))
            sys.exit(400)
        else:
            logging.error("Sin conectividad al RDS: {}. Detalle: {}".format(instance_data['Endpoint']['Address'], e))
            sys.exit(500)


def get_tables_with_results(instance_data, db="postgres"):
    conn = open_conn(db, instance_data)
    try:
        if conn:
            cur = conn.cursor()
            cur.execute(open(file, "r").read())
            result = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            res_list = []
            for each_res in tuple(result):
                res_list.append(dict(zip(column_names, each_res)))
            cur.close()
            conn.close()
            return res_list
    except psycopg2.ProgrammingError as e:
        if "cur" in locals():
            logging.info("No results to fetch in host {}: Status Message: {}"
                         .format(instance_data['Endpoint']['Address'], cur.statusmessage))
        return None
    except psycopg2.errors.InsufficientPrivilege as e:
        if "permission denied for relation" in str(e):
            logging.info("No permissions in relation for host {} and user {}. Error: {}"
                         .format(instance_data['Endpoint']['Address'], user, e))
        return None
    return None

def is_master(i):
    return "ReadReplicaSourceDBInstanceIdentifier" not in i.keys()


def process_instance(instance):
    """
    Recorro las instancias en búsqueda de replicas.
    Luego, guardo datos actuales.
    """
    message = ""
    temp_dict = {}
    i = instance['DBInstances'][0]

    if is_master(i):
        db = "postgres"
        res = get_tables_with_results(i)
        if res:
            temp_dict['database'] = db
            temp_dict['identifier'] = str(i['DBInstanceIdentifier'])
            temp_dict['endpoint'] = str(i['Endpoint']['Address'])
            temp_dict['result'] = "*"
            temp_dict['result'] = temp_dict['result'] + ",".join(list(res[0].keys())) + "*"
            for each_dict in res:
                temp_dict['result'] = temp_dict['result'] + "\n"
                for each_key in each_dict.keys():
                    temp_dict['result'] = temp_dict['result'] + each_dict[each_key] + ","
            message = message + format_check(temp_dict)
    if message:
        body = "<h2>There are results for query {}</h2><ul>".format(filename)
        body += message
        body += "</ul>"
        print(res)

        if args.receivers:
            subject = str(fullprofile).upper() + ' - ' + rds_instance + ' - {} response'.format(filename)
            gm = gmail.Gmail('rodri.cadaval@gmail.com')
            for an_email in list_users:
                gm.send_message(an_email, subject, body)


def get_rds_list(throttle=0.2):
    """
  Get list of all the DBs from DocDB
  """
    try:
        rdsclient = boto3.client('rds')
        paginator = rdsclient.get_paginator('describe_db_instances')
        filter_list = []
        filter_list.append({'Name': 'engine', 'Values': [rds_engine]})
        pages = paginator.paginate(DBInstanceIdentifier=rds_instance, Filters=filter_list)

        for page in pages:
            process_instance(page)
        print()
    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return get_rds_list(throttle*2)
        else:
            pprint(traceback.format_exc())



def main():
    check_secret()
    get_rds_list()


if __name__ == '__main__':
    main()

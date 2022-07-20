#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform custom queries in each database in a groups or an individual RDS

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

os.system('clear')

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

parser = argparse.ArgumentParser(description='Checking Table-Databases Showing Custom Queries Results')

parser.add_argument('--profile', action="store", dest="profile", help="xx-xx-east-1 | xx-xx-west-2 | xx1-xx-east-1 | "
                                                                      "xx1-xx-west-2 | xxx-xxx-east-1 | xxx-xxx-west-2 | "
                                                                      "xx1-xx-east-1 | xxxx-xx-west-2",
                    required=True, type=valid_profile)
parser.add_argument('-u', '--user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication")
parser.add_argument('-p', '--password', action="store", dest="secret", help="Database Name",
                    required=False)
parser.add_argument('--no-opsgenie', action="store_true", dest="no_opsgenie", help="Send opsgenie alert",
                    required=False)
parser.add_argument('-r', '--rds-instance', action="store", dest="instance", default="postgres", help="Engine",
                    required=False)
parser.add_argument('-e', '--engine', action="store", dest="engine", default="postgres", help="Engine",
                    required=False)
parser.add_argument('-f', '--file', action="store", dest="file", help="File path",
                    required=True)
parser.add_argument('-l', '--list-users', action="store", dest="receivers", help="User list separated by comma",
                    required=False)
parser.add_argument('--cose-alerts', action="store_true", dest="close_alerts", help="Cerrar todas las alertas de "
                                                                                  "opsgenie",
                    required=False)


args = parser.parse_args()

fullprofile = args.profile
rds_engine = args.engine
user = args.rep_user
rds_instance = args.instance
file = args.file
filename = os.path.basename(file)
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
        if args.no_opsgenie:
            message = "<li><b>" + data['database'] + "</b><ul>"
            for each_key in data:
                if each_key != "database":
                    message = message + "<li> "+each_key+": " + str(data[each_key]) + "</li>"
            message = message + "</ul></li>"
        else:
            message = data['database']
            for each_key in data:
                if each_key != "database":
                    message = message + " \n \t"+each_key+": " + str(data[each_key]) + "\n "
            message = message + "\n\n"
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


def get_tables_with_results(db, instance_data):
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


def list_databases(instance_data):
    try:
        global secret
        my_connection = open_conn("postgres", instance_data)
        cur = my_connection.cursor()
        cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE datallowconn AND "
                    "datname NOT IN ('{}',"
                    "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                    "'dba_%|%delete_me' AND pg_database_size(datname) > 220000000 ORDER BY pg_database_size(datname) "
                    "DESC) as bla;".format(
            'rdsadmin',
                                                                                                 'template1',
                                                                                    'template0',
                                                                       'postgres'))
        result = cur.fetchall()
        cur.close()
        my_connection.close()
        return result[0][0]
    except psycopg2.OperationalError as e:
        pprint("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)
    except Exception as e:
        pprint("{}".format(e))


def is_master(i):
    return "ReadReplicaSourceDBInstanceIdentifier" not in i.keys()


def process_instance(instance):
    """
    Recorro las instancias en búsqueda de replicas.
    Luego, guardo datos actuales.
    """
    message = ""

    i = instance['DBInstances'][0]

    if is_master(i):
        temp_dict = {}
        dbs = list_databases(i)
        if dbs:
            for db in dbs:
                res = get_tables_with_results(db, i)
                if res:
                    if str(i['DBInstanceIdentifier']) not in temp_dict.keys():
                       temp_dict[i['DBInstanceIdentifier']] = dict()
                    for each_dict in res:
                        for key in each_dict:
                            temp_dict[i['DBInstanceIdentifier']][key] = str(each_dict[key])
                    each_dict['database'] = db
                    each_dict['identifier'] = str(i['DBInstanceIdentifier'])
                    each_dict['endpoint'] = str(i['Endpoint']['Address'])
                    message = message + format_check(each_dict)
    if message:
        if args.no_opsgenie:
            body = "<h2>Please review this tables. There are results for query {}</h2><ul>".format(filename)
            body += message
            body += "</ul>"
        else:
            body = "Please review this tables. There are custom query results \n\n"
            body += message

        if not args.no_opsgenie:
            opsg = opsgenie.Opsgenie()
            link = 'https://X1Xdev.atlassian.net/wiki/spaces/PDP/pages/2620130736/pg-schema-change+Int+a+Bigint+clean-table+PostgreSQL+9.6-13'
            opsg.set_message("[custom-query] [{}] Table/s in rds: {} responding custom query".format(fullprofile, i[
                                                                                                             'DBInstanceIdentifier']))
            opsg.set_description("[Suggest] Realizar la clonacion e intercambio de tablas a traves de pg-schema-change "
                                 " o realizar el ALTER TABLE directamente sobre la tabla en caso de ser pequeña"
                                 "->  "
                                 "{}\n\n"
                                 "[Suggest] Recuerda que es recomendable reducir el size de la tabla coordinando con los "
                                 "owners del ms.\n\n"
                                     "Listado: {}".format(link, message))
            opsg.set_alias("custom-query-{}".format(str(i['DBInstanceIdentifier'])))
            opsg.set_details({"script-path": os.path.realpath(__file__), "user": getpass.getuser()})
            opsg.create()
        if args.receivers:
            subject = str(fullprofile).upper() + ' - ' + rds_instance + ' - {} response'.format(filename)
            gm = gmail.Gmail('rodri.cadaval@gmail.com')
            for an_email in list_users:
                gm.send_message(an_email, subject, body)


def close_alerts():
    opsg = opsgenie.Opsgenie()
    opsg.close_listed_alerts("tag=RDS and tag=dba-monitor and tag=custom")


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
        return pprint("Finished")
    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return get_rds_list(throttle*2)
        else:
            pprint(traceback.format_exc())



def main():
    if args.close_alerts:
        close_alerts()
    else:
        check_secret()
        get_rds_list()


if __name__ == '__main__':
    main()

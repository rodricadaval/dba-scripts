#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform auto-scaling to docdb replicas on nightshifts.
# Collect vital stats when needed

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
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import opsgenie
from lib import dynamo
import logging
import getpass
import traceback
import psycopg2.extras


os.system('clear')

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('psycopg2').setLevel(logging.INFO)
logging.basicConfig(level = logging.INFO)

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

parser = argparse.ArgumentParser(description='Checking Table-Databases Reaching Integer Limit')

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
parser.add_argument('--cose-alerts', action="store_true", dest="close_alerts", help="Cerrar todas las alertas de "
                                                                                  "opsgenie",
                    required=False)

args = parser.parse_args()

fullprofile = args.profile
rds_engine = args.engine
user = args.rep_user
rds_instance = args.instance

if args.secret is not None:
    secret = args.secret

boto3.setup_default_session(profile_name=fullprofile)

slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXX"
slackChannel = "dba_boto3_reports"


def sendToSlack(var_data):
    """
    Slack
    """
    color = "#FF164f"
    thumb = "https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", \
            "author_name": "Alert sequences reaching max integer value", \
            "fields": [ \
            {"title": "Schema.table.column", "value": "' + str(var_data[0]) + '", "short": true}, \
            {"title": "Last value", "value": "' + str(var_data[1]) + '","short": true}, \
            {"title": "Increments by","value": "' + str(var_data[2]) + '","short": true}, \
            {"title": "Database","value": "' + str(var_data[5]).replace("\"","") + '","short": true}, \
            {"title": "Remaining ids","value": "' + str(var_data[4]) + '","short": true}, \
            {"title": "RDS","value": "' + str(var_data[6]) + '","short": true}, \
            {"title": "AWS Account","value": "' + str(fullprofile) + '","short": true}, \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "last check", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-RDS-integer-limit", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


def format_seq_check(data):
    try:
        message = ""
        sendToSlack(data)
        if args.no_opsgenie:
            message = "<li><b>" + data[6] + "</b><ul><li> Table: " + str(data[0]) + "</li><li>Value: <b>" \
                      + str(data[1]) + "</b></li><li>Remaining: <b>" + str(data[4]) + "</b></li> <li> Incrementing by: " \
                      + str(data[2]) + "</li><li>Database: " + str(data[5]) + "</li><li>RDS: " + str(data[6]) + "</li> " \
                                                                                                               "</ul></li>"
        else:
            message = data[6] + " \n \tTable: " + str(data[0]) + "\n " \
                                                                 "\tValue: " + str(data[1]) + "\n " \
                                                                 "\tRemaining: " + str(data[3]) + "\n " \
                                                                 "\tIncrementing by: " + str(data[2]) + "\n " \
                                                                 "\tDatabase: " + str(data[5]) + "\n " \
                                                                 "\tRDS: " + str(data[6]) + "\n\n"
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
                .format(db.replace("\"",""), instance_data['Endpoint']['Address'], user, secret,
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


def critical_value(tup):
    if tup[1]:
        last_value = int(tup[1])
        increment_by = int(tup[2])
        if str(tup[3]) == 'integer':
            if last_value < 0 and increment_by < 0:
                return 2147483647 + last_value
            elif last_value < 0 and increment_by > 0:
                return -1 * last_value
            elif last_value > 0 and increment_by < 0:
                return last_value
            elif last_value > 0 and increment_by > 0:
                return 2147483647 - last_value
        elif str(tup[3]) == 'bigint':
            if last_value < 0 and increment_by < 0:
                return 9223372036854775807 + last_value
            elif last_value < 0 and increment_by > 0:
                return -1 * last_value
            elif last_value > 0 and increment_by < 0:
                return last_value
            elif last_value > 0 and increment_by > 0:
                return 9223372036854775807 - last_value
    return None


def get_tables_in_danger(db, instance_data):
    conn = open_conn(db, instance_data)
    try:
        if conn:
            cur = conn.cursor()
            cur2 = conn.cursor()
            cur.execute("SELECT CASE WHEN POSITION('.' in SPLIT_PART(column_default, '''', 2)) = 0 THEN table_schema "
                        "ELSE SPLIT_PART(SPLIT_PART(column_default, '''', 2),'.',1) end as sequence_schema, "
                        "CASE WHEN POSITION('.' in SPLIT_PART(column_default, '''', 2)) = 0 "
                        "THEN SPLIT_PART(column_default, '''', 2) "
                        "ELSE SPLIT_PART(SPLIT_PART(column_default, '''', 2),'.',2) "
                        "end as sequence_name, table_schema, table_name, column_name, data_type "
                        "FROM information_schema.columns "
                        "WHERE column_default ilike 'nextval%' AND data_type IN ('integer', 'bigint') AND table_schema "
                        "NOT IN ('information_schema', 'pg_catalog', 'pglogical', 'topology', 'tiger', 'logger') "
                        "ORDER BY table_schema, table_name;")
            seq_list = []
            for each_res in cur.fetchall():
                if int(instance_data['EngineVersion'].split(".",1)[0]) < 10:
                    cur2.execute("SELECT '{}.{}.{}', last_value, increment_by, '{}' FROM {}.{}".format(each_res[2],
                                                                                                      each_res[3],
                                                                                                      each_res[4],
                                                                                                      each_res[5],
                                                                                                      each_res[0],
                                                                                                      each_res[1]))
                else:
                    cur2.execute("SELECT '{}.{}.{}', last_value, increment_by, '{}' FROM pg_sequences "
                                 "WHERE schemaname = '{}' AND sequencename = '{}'".format(each_res[2].replace("\"", ""),
                                                                                          each_res[3], each_res[4],
                                                                                          each_res[5],
                                                                                          each_res[0].replace("\"", ""),
                                                                                          each_res[1]))
                seq_state = cur2.fetchone()
                ids_left = critical_value(seq_state)
                if ids_left:
                    if ids_left < 300000000:
                        seq_list.append(seq_state + (ids_left,))
            cur.close()
            if "cur2" in locals():
                cur2.close()
            conn.close()
            return seq_list
    except psycopg2.errors.InsufficientPrivilege as e:
        if "permission denied for relation" in str(e):
            logging.info("No permissions in relation for host {} and user {}. Error: {}"
                         .format(instance_data['Endpoint']['Address'], user, e))
        return None
    except psycopg2.errors.SyntaxError as e:
        logging.error("Database: {}, Error: {}".format(db, traceback.format_exc()))
    except Exception as e:
        logging.error("Error: {}, traceback: {}".format(e, traceback.format_exc()))
        return None


def list_databases(instance_data):
    try:
        global secret
        my_connection = open_conn("postgres", instance_data)
        cur = my_connection.cursor()
        cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE datallowconn AND "
                    "datname NOT IN ('{}',"
                    "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                    "'dba_%|%delete_me' ORDER BY pg_database_size(datname) "
                    "DESC) as bla;".format('rdsadmin', 'template1', 'template0', 'postgres'))
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
                res = get_tables_in_danger(db, i)
                if res:
                    if str(i['DBInstanceIdentifier']) not in temp_dict.keys():
                       temp_dict[i['DBInstanceIdentifier']] = dict()
                    for each_seq in res:
                        temp_dict[i['DBInstanceIdentifier']]["database"] = db
                        temp_dict[i['DBInstanceIdentifier']]["table"] = str(each_seq[0])
                        temp_dict[i['DBInstanceIdentifier']]["actual_value"] = each_seq[1]
                        temp_dict[i['DBInstanceIdentifier']]["increments_by"] = each_seq[2]
                        temp_dict[i['DBInstanceIdentifier']]["ids_left"] = each_seq[4]
                        each_seq = each_seq + (db, str(i['DBInstanceIdentifier']), str(i['Endpoint'][
                            'Address']),)
                        message += format_seq_check(each_seq)
    if message:
        if args.no_opsgenie:
            body = "<h2>Please review this tables. There are columns reaching max integer values</h2><ul>"
            body += message
            body += "</ul>"
        else:
            body = "Please review this tables. There are columns reaching max integer values \n\n"
            body += message

        subject = str(fullprofile).upper() + '- Databases reaching max integer value'
        opsg = opsgenie.Opsgenie()
        link = 'https://X1Xdev.atlassian.net/wiki/spaces/PDP/pages/2620130736/pg-schema-change+Int+a+Bigint+clean-table+PostgreSQL+9.6-13'
        opsg.set_message("[int-to-bigint] [{}] Table/s in rds: {} reaching max integer value".format(fullprofile, i[
                                                                                                         'DBInstanceIdentifier']))
        opsg.set_description("[Suggest] Realizar la clonacion e intercambio de tablas a traves de pg-schema-change "
                              "->  "
                             "{}\n\n"
                             "[Suggest] Recuerda que es recomendable reducir el size de la tabla coordinando con los "
                             "owners del ms.\n\n"
                                 "Listado: {}".format(link, message))
        opsg.set_alias("int-to-bigint-{}".format(str(i['DBInstanceIdentifier'])))
        opsg.set_details({"script-path": os.path.realpath(__file__), "user": getpass.getuser()})
        opsg.create()


def close_alerts():
    opsg = opsgenie.Opsgenie()
    opsg.close_listed_alerts("tag=RDS and tag=dba-monitor and tag=integer")


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
            traceback.format_exc()


def main():
    if args.close_alerts:
        close_alerts()
    else:
        check_secret()
        get_rds_list()


if __name__ == '__main__':
    main()

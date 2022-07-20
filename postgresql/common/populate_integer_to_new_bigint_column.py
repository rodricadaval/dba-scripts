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
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content
import smtplib, ssl
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
sys.path.insert(0, git_root + '/mongodb/common')
import gmail
from lib import dynamo
import logging
from multiprocessing import Process, Queue, Manager


logging.basicConfig(level = logging.ERROR)

parser = argparse.ArgumentParser(description='Altering integer column to bigint')

parser.add_argument('--host', action="store", dest="db_host", help="Rds Endpoint",
                    required=True)
parser.add_argument('-d', '--database', action="store", dest="db", help="Database", required=True)
parser.add_argument('-s', '--schema', action="store", dest="db_schema", default="public",
                    help="Table Schema")
parser.add_argument('-t', '--table', action="store", dest="db_table", help="Table Name",
                    required=True)
parser.add_argument('-c', '--column', action="store", dest="db_column", help="Table column to alter",
                    required=True)
parser.add_argument('-u', '--user', action="store", dest="db_user", default="dba_test_service", help="User",
                    required=False)
parser.add_argument('-p', '--password', action="store", dest="db_secret", help="User Password",
                    required=False)
parser.add_argument('-q', '--quantity-of-rpb', action="store", dest="step", default=5000, help="Batches",
                    required=False)
parser.add_argument('-b', '--begin-with', action="store", dest="begin_with", default=1, help="Begin in value",
                    required=True)
parser.add_argument('-j', '--jobs', action="store", dest="jobs", default=1, help="Worker jobs to use",
                    required=False)

args = parser.parse_args()

db_host = args.db_host
db_user = args.db_user
db = args.db
db_schema = args.db_schema
db_table = args.db_table
db_column = args.db_column
begin_with = int(args.begin_with)
step = int(args.step)

def check_secret():
    if db_user == "dba_test_service":
        try:
            i_dynamo = dynamo.DynamoClient('dev')
            var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
            return var['value']
        except Exception as e:
            print("No se encontraron variables del user en dynamo dev, busco en prod")
            i_dynamo = dynamo.DynamoClient('xx')
            var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
            return var['value']

if args.db_secret is not None:
    secret = args.db_secret
else:
    secret = check_secret()
jobs = int(args.jobs)

def open_conn():
    conn = psycopg2.connect(
        "dbname='{}' host='{}' user='{}' password='{}'"
            .format(db, db_host , db_user, secret))
    conn.autocommit = True
    return conn

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
            {"title": "Database","value": "' + str(var_data[4]).replace("\"","") + '","short": true}, \
            {"title": "Remaining ids","value": "' + str(var_data[3]) + '","short": true}, \
            {"title": "RDS","value": "' + str(var_data[5]) + '","short": true}, \
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
                      + str(data[1]) + "</b></li><li>Remaining: <b>" + str(data[3]) + "</b></li> <li> Incrementing by: " \
                      + str(data[2]) + "</li><li>Database: " + str(data[4]) + "</li><li>RDS: " + str(data[5]) + "</li> " \
                                                                                                               "</ul></li>"
        else:
            message = data[6] + " \n \tTable: " + str(data[0]) + "\n " \
                                                                 "\tValue: " + str(data[1]) + "\n " \
                                                                 "\tRemaining: " + str(data[3]) + "\n " \
                                                                 "\tIncrementing by: " + str(data[2]) + "\n " \
                                                                 "\tDatabase: " + str(data[4]) + "\n " \
                                                                 "\tRDS: " + str(data[5]) + "\n\n"
        return message

    except Exception as e:
        pprint("Error:" + str(e))
        sys.exit(1)

def get_max_value(connection) -> int:
    try:
        with connection.cursor() as cur:
            cur.execute('SELECT max("{}") FROM "{}"."{}";'.format(db_column, db_schema, db_table))
            result = cur.fetchone()
        connection.close()
        if result is not None:
            return result[0]
        else:
            raise psycopg2.ProgrammingError("Error al consultar el max column value")
    except psycopg2.OperationalError as e:
        logging.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def need_vacuum(connection, hm_sleep=1):
    try:
        with connection.cursor() as cur:
            cur.execute('SELECT CASE '
                        'WHEN prg.datname IS NOT NULL THEN 0 '
                        'WHEN ut.n_dead_tup = 0 THEN 0 '
                        'WHEN ut.n_live_tup = 0 THEN 0 '
                        'WHEN ut.n_live_tup IS NULL THEN 0 '
                        'WHEN (ut.n_dead_tup * 100 / ut.n_live_tup) < 0.01 THEN 0 '                        
                        'ELSE 1 END AS need_vacuum, prg.datname, prg.relid::regclass::text, prg.heap_blks_total, '
                        'prg.heap_blks_scanned, prg.num_dead_tuples '
                        'FROM pg_stat_progress_vacuum prg LEFT JOIN pg_stat_user_tables ut ON ut.relname = '
                        'prg.relid::regclass::text AND prg.datname = \'{2}\' WHERE schemaname = \'{0}\' AND '
                        'relname = \'{1}\';'
                        .format(db_schema, db_table, db))
            result = cur.fetchone()
        if result is not None:
            if result[1]:
                pct = int(result[4]) / int(result[3]) * 100
                print("Waiting for vacuum to finish. Pct in progress: {}".format(str(pct)))
                if pct < 25 and hm_sleep < 60:
                    hm_sleep = hm_sleep * 4
                    time.sleep(hm_sleep)
                elif 25 <= pct < 50 and hm_sleep < 60:
                    hm_sleep = hm_sleep * 3
                    time.sleep(hm_sleep)
                elif 50 <= pct < 75 and hm_sleep < 60:
                    hm_sleep = hm_sleep * 2
                    time.sleep(hm_sleep)
                else:
                    hm_sleep = hm_sleep * 1.05
                    time.sleep(hm_sleep)
                need_vacuum(connection, hm_sleep)
            else:
                return result[0]
        else:
            return False
            # raise psycopg2.ProgrammingError("Error al consultar la cantidad de dead_tuples")
    except psycopg2.OperationalError as e:
        logging.error("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def execute_iteration(queue, latest_bigtable_id, workers):
    connection = open_conn()
    counter = int(queue)
    while counter <= latest_bigtable_id:
        with connection.cursor() as cursor:
            if need_vacuum(connection):
                print("Performing vacuum on: {}, time: {}".format(db_table, datetime.datetime.now()))
                cursor.execute('VACUUM ANALYZE "{}"."{}";'.format(db_schema, db_table))
            if counter + step >= 2147483611:
                max_val = 2147483611
            else:
                max_val = counter + step
            '''
            print('UPDATE "{0}"."{1}" SET "{2}" = "{3}" WHERE "{3}" >= {4} AND "{3}" < {5};'.format(db_schema,
                                                                                                   db_table,
                                                                                                   db_column + '_new',
                                                                                                   db_column,
                                                                                                   counter,
                                                                                                   max_val))
            '''
            cursor.execute('UPDATE "{0}"."{1}" SET "{2}" = "{3}" WHERE "{3}" >= {4} AND "{3}" < {5};'.format(db_schema,
                                                                                                   db_table,
                                                                                                   db_column + '_new',
                                                                                                   db_column,
                                                                                                   counter,
                                                                                                   max_val))
            print("Contador: {}, Max Value: {}".format(counter, latest_bigtable_id))
        counter = counter + (step * workers)
    connection.close()

def copy_bigtable_id(counter):
    latest_bigtable_id = get_max_value(open_conn())

    queue = Manager().Queue()

    for i in range(jobs):
        queue.put(counter+(i*step))

    processes = [Process(target=execute_iteration, args=(queue.get(), latest_bigtable_id, jobs, )) for _ in
                 range(jobs)]

    for p in processes:
        p.daemon = True
        p.start()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
        sys.exit(1)

    next_iteration = counter+((jobs-1)*step)

    processes.clear()

    if next_iteration < latest_bigtable_id:
        copy_bigtable_id(next_iteration)
    else:
        print("Finished")

def main():
    copy_bigtable_id(begin_with)

if __name__ == '__main__':
    main()

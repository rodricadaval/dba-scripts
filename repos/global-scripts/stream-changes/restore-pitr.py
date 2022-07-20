#!/usr/bin/python3

import boto3
import json
import random
import calendar
import time
import os
import sys
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from psycopg2.extensions import AsIs
import argparse
import urllib
import logging
from botocore.exceptions import ClientError
from io import StringIO
import glob
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
import threading
import multiprocessing
from boto3.s3.transfer import TransferConfig, S3Transfer
from botocore.config import Config
import datetime
import pprint
import shlex

STREAM_NAME = 'binlog-backup'
S3_BUCKET = 's3://binlog-backup'
LOGS = '/var/log/X1X/backups'
BUCKET_NAME = 'binlog-backup'
s3 = None
file_name = None
file_path = None
oldepoch = None
fake_lines = []

logging.basicConfig(level=logging.INFO)

def valid_profile(s):
    try:
        if s in ["xx-xx-east-1", "xx-xx-west-2", "xx1-xx-east-1", "xx1-xx-west-2", "xxx-xxx-east-1",
                 "xxx-xxx-west-2",
                 "xx1-xx-east-1", "xxxx-xx-west-2"]:
            return s
        else:
            raise ValueError(s)
    except ValueError:
        msg = "Not a valid profile: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser(description='Stream changes to Kinesis')

parser.add_argument('-s', '--source', action="store", dest="engine_host", help="AWS Instance Endpoint", required=True)
parser.add_argument('-d', '--database', action="store", dest="database", help="Databases (comma separated) | 'all' "
                                                                              "accepted",
                    required=True)
parser.add_argument('-u', '--user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication (optional)")
parser.add_argument('-p', '--password', action="store", dest="secret", help="Role secret (optional)",required=False)
parser.add_argument('-a', '--action', action="store", dest="action", help="Action to execute", required=True)
parser.add_argument('-f', '--from-time', action="store", dest="var_from", help="%Y%m%d-%H%M%S in UTC time",
                    required=True)
parser.add_argument('-t', '--to-time', action="store", dest="var_to", help="%Y%m%d-%H%M%S in UTC time", required=True)
parser.add_argument('--profile', action="store", dest="profile", help="xx-xx-east-1 | xx-xx-west-2 | xx1-xx-east-1 | "
                                                                      "xx1-xx-west-2 | xxx-xxx-east-1 | xxx-xxx-west-2 | "
                                                                      "xx1-xx-east-1 | xxxx-xx-west-2",
                    required=True, type=valid_profile)

args = parser.parse_args()

engine_host = args.engine_host
if args.database is not None:
    arg_db = args.database
    list_dbs = [x.strip() for x in arg_db.split(',')]
user = args.rep_user
if args.secret is not None:
    secret = args.secret
action = args.action
var_from = args.var_from
var_to = args.var_to

AWS_PROFILE = args.profile
AWS_REGION = AWS_PROFILE.split("-",1)[1]

api_config = Config(
    region_name=AWS_REGION,
    max_pool_connections=200,
    connect_timeout=5,
    read_timeout=300,
    retries={
        'max_attempts': 5,
        'mode': 'standard'
    }
)

session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client('s3', config=api_config)
RDS_NAME = os.popen("nslookup " + engine_host + " | grep Name | cut -d: -f2 | xargs").readline().strip()

def check_val(s):
    if isinstance(s, int):
        if s == 0:
            return True
        return True
    elif isinstance(s, bool):
        return s
    elif isinstance(s, float):
        return True
    elif isinstance(s, type(None)):
        return s
    elif isinstance(s, str):
        return True
    else:
        return s

def get_statement(obj):
    if obj['kind'] == "insert":
        return 'INSERT INTO "{}"."{}" ({}) VALUES ({});'.format(obj['schema'],
                                                                  obj['table'],
                                                                  ",".join(['"{0}"'.format(s) for s in obj[
                                                                      'columnnames']]),
                                                                  ",".join(
                                                                      ["'{0}'".format(s) if check_val(s) else 'NULL' for
                                                                       s in
                                                                       obj[
                                                                           'columnvalues']])
                                                                  )
    elif obj['kind'] == "update":
        modified_keys = dict(zip(obj['oldkeys']['keynames'], obj['oldkeys']['keyvalues']))
        return 'UPDATE "{}"."{}" ' \
               'SET ({}) = ({}) WHERE ' \
               '{};'.format(obj['schema'],
                              obj['table'],
                              ",".join(['"{0}"'.format(s) for s in obj['columnnames']]),
                              ",".join(["'{0}'".format(s) if check_val(s) else 'NULL' for s in obj[
                                  'columnvalues']]),
                              " AND ".join(["\"{}\" = %s".format(k, ("\'" + str(modified_keys[k]) + "\'")
                              if check_val(modified_keys[k]) else 'NULL')
                                            for k in modified_keys])
                              )
    elif obj['kind'] == "delete":
        modified_keys = dict(zip(obj['oldkeys']['keynames'], obj['oldkeys']['keyvalues']))
        return 'DELETE FROM "{}"."{}" ' \
               'WHERE {};'.format(obj['schema'],
                                    obj['table'],
                                    " AND ".join(["\"{}\" = %s".format(k, ("\'" + str(modified_keys[k]) + "\'")
                                    if check_val(modified_keys[k]) else 'NULL')
                                            for k in modified_keys])
                                    )


def get_statement2(obj):
    if obj['kind'] == "insert":
        return [('INSERT INTO "{}"."{}" ({}) VALUES ({});'.format(obj['schema'],
                                                                  obj['table'],
                                                                  ",".join(['"{0}"'.format(s) for s in obj[
                                                                      'columnnames']]),
                                                                  ",".join(
                                                                      ["%s".format(s) for s in obj['columnvalues']]))),
                                                                 tuple(s if check_val(s) else None for s in obj[
                                                                     'columnvalues'])]
    elif obj['kind'] == "update":
        modified_keys = dict(zip(obj['oldkeys']['keynames'], obj['oldkeys']['keyvalues']))
        return [('UPDATE "{}"."{}" ' \
               'SET ({}) = ({}) WHERE ' \
               '{};'.format(obj['schema'],
                              obj['table'],
                              ",".join(['"{0}"'.format(s) for s in obj['columnnames']]),
                              ",".join(["%s".format(s) for s in obj[
                                  'columnvalues']]),
                              " AND ".join(["\"{}\" = %s".format(k) for k in modified_keys]))
                            ), tuple(s if check_val(s) else None for s in obj['columnvalues']) +
                                      tuple(modified_keys[k] if check_val(modified_keys[k])
                                           else None for k in modified_keys)]
    elif obj['kind'] == "delete":
        modified_keys = dict(zip(obj['oldkeys']['keynames'], obj['oldkeys']['keyvalues']))
        return [('DELETE FROM "{}"."{}" WHERE {};'.format(obj['schema'],
                                    obj['table'],
                                    " AND ".join(["\"{}\" = %s".format(k) for k in modified_keys])
                                    )), tuple(modified_keys[k] if check_val(modified_keys[k]) else None for k in
                                        modified_keys)]


def format_sql(python_dict):
    sql = ""
    for obj in python_dict:
        res = get_statement2(obj)
        sql += str(res[0] + "," + str(res[1]))
        # Adding CR
        sql += '\n'
    return sql

def open_conn(db):
    global secret
    conn = psycopg2.connect(
        "dbname='{}' host='{}' user='{}' password='{}'"
            .format(db, engine_host, user, secret))
    conn.autocommit = False
    return conn


def perform(db, fake_lines, bucket_file, restore):
    global secret
    try:
        conn = open_conn(db)
        cur = conn.cursor()

        restore_file_path = "{}/global-scripts/stream-changes/restore_files/{}/from_{}_to_{}".format(
            git_root, engine_host, var_from.replace("-", "_"), var_to.replace("-", "_"))
        with open("{}/{}.restore".format(restore_file_path, str(db)), "a") as new_file:
            new_file.write("\n-- FILE: {}\n".format(bucket_file))
            i = 0
            while i < len(fake_lines):
                line_object = json.loads(fake_lines[i])
                if line_object['database'] == str(db):
                    for obj in line_object['change']:
                        statement = get_statement2(obj)
                        query = cur.mogrify((statement[0]), statement[1])
                        new_file.write(query.decode("utf-8")  + '\n')
                        if restore:
                            cur.execute(query)
                i += 1
        conn.commit()
    except psycopg2.OperationalError as e:
        logging.error("Sin conectividad al RDS. Detalle: {}".format(e))
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.rollback()
            conn.close()
        sys.exit(504)
    except psycopg2.InterfaceError as e:
        sys.exit(504)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error executing query: {}, file: {}, db: {}".format(str(error), bucket_file, db))
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.rollback()
            logging.error("Performing rollback on file: {}, db: {}".format(bucket_file, db))
            conn.close()
        sys.exit(404)
    finally:
        cur.close()
        conn.close()

def list_databases():
    global secret
    try:
        my_connection = psycopg2.connect(
            "dbname='{}' host='{}' user='{}' password='{}'".format("postgres", engine_host, user, secret))
        cur = my_connection.cursor()
        cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE datallowconn AND "
                    "datname NOT IN ('{}',"
                    "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                    "'dba_%|%delete_me%' ORDER BY pg_database_size(datname) DESC LIMIT 150) as bla;".format(
            'rdsadmin', 'template1', 'template0', 'postgres'))
        result = cur.fetchall()
        cur.close()
        return result[0][0]
    except psycopg2.OperationalError as e:
        logging.info("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)

def check_secret():
    global secret
    if user == "dba_test_service":
        i_dynamo = dynamo.DynamoClient('dev')
        var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
        secret = var['value']

def check_dates(var_from, var_to):
    format = "%Y%m%d-%H%M%S"

    try:
        datetime.datetime.strptime(var_from, format)
        datetime.datetime.strptime(var_to, format)
    except ValueError:
        parser.error("Date FROM -f or date TO -t format is not valid. Please use this format %Y%m%d-%H%M%S")

class DateParser:
    def __init__(self, date):
        self.day = int(date.split("-")[0])
        self.time = int(date.split("-")[1])
        self.datetime = int(date.replace("-",""))

def generate_restore_folder():
    path = "{}/global-scripts/stream-changes/restore_files/{}/from_{}_to_{}".format(
        git_root, engine_host, var_from.replace("-", "_"), var_to.replace("-", "_"))
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        files = glob.glob('{}/*'.format(path))
        for f in files:
            os.remove(f)

def alter_role_timeouts():
    global secret
    try:
        conn = open_conn("postgres")
        cur = conn.cursor()
        query = cur.mogrify('ALTER ROLE "%s" SET statement_timeout = 0', [AsIs(user)])
        cur.execute(query)
        query = cur.mogrify('ALTER ROLE "%s" SET idle_in_transaction_session_timeout = 0', [AsIs(user)])
        cur.execute(query)
        query = cur.mogrify('ALTER ROLE "%s" SET lock_timeout = 0', [AsIs(user)])
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        logging.info("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)


def main():
    """
    Starts streaming
    """
    global fake_lines

    check_secret()
    check_dates(var_from, var_to)

    paginator = s3_client.get_paginator('list_objects')
    operation_parameters = {'Bucket': BUCKET_NAME,
                            'Prefix': 'pg/{}/'.format(RDS_NAME)}
    page_iterator = paginator.paginate(**operation_parameters)
    filtered_files = []
    for page in page_iterator:
        for each in page['Contents']:
            each_date = DateParser(each['Key'].split("/")[2])
            parse_from = DateParser(var_from)
            parse_to = DateParser(var_to)
            if parse_from.day == each_date.day:
                if parse_from.datetime <= each_date.datetime <= parse_to.datetime:
                    filtered_files.append(each['Key'])

    if action in ["restore", "verbose"]:
        # create a list of threads
        threads = []
        if "arg_db" in globals():
            dbs = list_dbs
        if str(arg_db).lower() == "all":
            dbs = list_databases()
        # In this case 'dbs' is a list of databases that discarding internal and delete_me waste.
        generate_restore_folder()
        if "dba" in user:
            alter_role_timeouts()
        for file in filtered_files:
            threads.clear()
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file)
            fake_file = StringIO(obj['Body'].read().decode('utf-8'))
            fake_lines = fake_file.readlines()
            for i in dbs:
                db = i.replace("\"", "")
                # We start one thread per database present.
                if action == "restore":
                    process = threading.Thread(target=perform, args=[db, fake_lines, file, True], name=db)
                elif action == "verbose":
                    process = threading.Thread(target=perform, args=[db, fake_lines, file, False], name=db)
                process.start()
                logging.info('Starting thread name:{}, status:{}, id:{}, file:{}'.format(process.name,
                                                                                   process.is_alive(),
                                                                            process.ident, file))
                threads.append(process)
            # We now pause execution on the main thread by 'joining' all of our started threads.
            # This ensures that each has finished processing the databases.
            for process in threads:
                process.join()
                # At this point, results for each DATABASE are being stored in a temporal file that acts like a buffer.
        logging.info('Finished restoring data.')
    else:
        print("There are no actions with this name")

if __name__ == '__main__':
    main()
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
import secrets
import string

logging.basicConfig(level=logging.INFO)
FORMATTER = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

my_logger = logging.getLogger()

parser = argparse.ArgumentParser(description='Create a new role in RDS')


parser.add_argument('--country', action="store", dest="db_country", help="Rds Endpoint",
                    required=False)
parser.add_argument('--rds', action="store", dest="rds", help="Rds Identifier",
                    required=False)
parser.add_argument('--host', action="store", dest="db_host", help="Rds Endpoint",
                    required=False)

parser.add_argument('--connection-user', action="store", dest="db_user", default="dba_test_service", help="User",
                    required=False)
parser.add_argument('--new-role', action="store", dest="db_new_role", help="New username", required=True)
parser.add_argument('--new-role-secret', action="store", dest="db_new_secret", help="New username secret", required=False)
parser.add_argument('-p', '--connection-password', action="store", dest="db_secret", help="Connection User Password",
                    required=False)
parser.add_argument('--connection-limit', action="store", default=100, help="Max connections for role",
                    required=False)
parser.add_argument('--statement-timeout', action="store", default=60000, help="In miliseconds",
                    required=False)
parser.add_argument('--lock-timeout', action="store", default=30000, help="In miliseconds",
                    required=False)
parser.add_argument('--idle-in-transaction-session-timeout', action="store", default=120000, help="In miliseconds",
                    required=False)
parser.add_argument('--work-mem', action="store", default=64, help="In MB",
                    required=False)
parser.add_argument('--replication', action="store_true", help="Give user replication grants",
                    required=False)
parser.add_argument('--superuser', action="store_true", help="Give user replication grants",
                    required=False)
parser.add_argument('--group-role', action="store", dest="db_group_role", help="Group Role Name", required=True)

args = parser.parse_args()

if not (args.db_country and args.rds) and not (args.db_host):
    parser.print_help(sys.stderr)
    sys.exit(100)
if args.db_country:
    country = args.db_country
else:
    country = ""
if args.rds:
    rds = args.rds
else:
    rds = ""
if args.db_host:
    db_host = args.db_host
else:
    db_host = ""
db_user = args.db_user
db_group_role = args.db_group_role
db_new_role = args.db_new_role

__qry_role_exists = """
SELECT
rolname
FROM pg_roles 
WHERE rolname = '{}'
""".format(str(db_new_role))

__qry_group_role_exists = """
SELECT
rolname
FROM pg_roles 
WHERE rolname = '{}'
""".format(str(db_group_role))


def search_instance_in_dynamo(instance):
    dynamordscli = dynamo_rds.DynamoRDSClient(str(country).lower())
    item = dynamordscli.get_vars(instance)
    if item:
        return json.loads(item['value'])
    return None


def open_conn():
    global rds, db_host, country
    if not args.db_host:
        instance = search_instance_in_dynamo(rds)
        db_host = instance['Endpoint']['Address']
        for tag in instance['TagList']:
            if tag['Key'] == "country":
                country = tag['Value']
    else:
        source_endpoint = os.popen("nslookup " + db_host + " | grep Name | cut -d: -f2 | xargs").readline().strip()
        rds = source_endpoint.split(".")[0]
    conn = psycopg2.connect(
        "dbname='postgres' host='{}' user='{}' password='{}'"
            .format(db_host, db_user, secret))
    conn.autocommit = True
    return conn


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


def get_slack_url():
    try:
        i_dynamo = dynamo.DynamoClient('dev')
        var = json.loads(i_dynamo.get_vars('dba_slack_url_hook'))
        return var['value']
    except Exception as e:
        print("No se encontraron variables de dba slack en dynamo dev, busco en prod")
        i_dynamo = dynamo.DynamoClient('xx')
        var = json.loads(i_dynamo.get_vars('dba_slack_url_hook'))
        return var['value']

slackUrl = get_slack_url()
slackChannel = "dba_boto3_reports"


def sendToSlack():
    """
    Slack
    """
    color = "#36a64f"
    thumb = "https://icon-library.com/images/icon-success/icon-success-17.jpg"

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", \
            "author_name": "Created role -> '+str(db_new_role)+'", \
            "fields": [ \
            {"title": "RDS", "value": "'+str(rds)+'", "short": true}, \
            {"title": "Country", "value": "' + str(country) + '", "short": true}, \
            {"title": "Connection Limit", "value": "'+str(args.connection_limit)+'", "short": true}, \
            {"title": "Group Role","value": "'+str(db_group_role)+'", "short": true}, \
            {"title": "Statement Timeout","value": "'+str(args.statement_timeout)+'", "short": true}, \
            {"title": "Lock Timeout","value": "' + str(args.lock_timeout) + '", "short": true}, \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "last check", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-RDS-USER-CREATOR", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


def create_user(conn):
    with conn.cursor() as cursor:
        my_logger.info("Creating role {}".format(db_new_role))
        if args.db_new_secret:
            secret = args.db_new_secret
        else:
            # secure password
            secret = ''.join((secrets.choice(string.ascii_letters + string.digits) for i in range(30)))
            print("Generated Secret: {}".format(secret))

        cursor.execute("CREATE ROLE \"{}\" LOGIN PASSWORD '{}' INHERIT;".format(db_new_role, secret))
        cursor.execute("ALTER ROLE \"{}\" connection limit {};".format(db_new_role, args.connection_limit))
        cursor.execute("ALTER ROLE \"{}\" SET statement_timeout={};".format(db_new_role, args.statement_timeout))
        cursor.execute("ALTER ROLE \"{}\" SET lock_timeout={};".format(db_new_role, args.lock_timeout))
        cursor.execute("ALTER ROLE \"{}\" SET idle_in_transaction_session_timeout={}; "
                       .format(db_new_role, args.idle_in_transaction_session_timeout))
        cursor.execute("ALTER ROLE \"{}\" SET work_mem='{}MB';".format(db_new_role, str(args.work_mem)))
        cursor.execute("ALTER ROLE \"{}\" SET random_page_cost='1.1';".format(db_new_role))
        cursor.execute("GRANT \"{}\" TO \"{}\";".format(db_group_role, db_new_role))
        cursor.execute("GRANT \"{}\" TO postgres;".format(db_new_role))
        if args.replication:
            cursor.execute("GRANT rds_replication TO \"{}\";".format(db_new_role))
        if args.superuser:
            cursor.execute("GRANT rds_superuser TO \"{}\";".format(db_new_role))
        my_logger.info("Finished making changes")
        sendToSlack()


def check_role_existance(conn):
    with conn.cursor() as cursor:
        cursor.execute(__qry_role_exists)
        result = cursor.fetchone()
        if result:
            my_logger.info("Role already exists. Nothing to do.")
            sys.exit(200)
        cursor.execute(__qry_group_role_exists)
        result = cursor.fetchone()
        if result:
            my_logger.info("Group role exists. Let's continue...")
        else:
            my_logger.error("El group role elegido no existe en el RDS. No es posible continuar.")
            sys.exit(499)


def main():
    """
    Actions to run
    """
    conn = open_conn()
    check_role_existance(conn)
    create_user(conn)


if __name__ == '__main__':
    main()

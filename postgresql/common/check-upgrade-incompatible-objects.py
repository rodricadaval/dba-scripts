#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform auto-scaling to docdb replicas on nightshifts.
# Collect vital stats when needed

import os
import json
import boto3
import psycopg2
from pprint import pprint
import sys
import argparse
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
import logging

os.system('clear')

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
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
                    help="Role that is going to perform queries")
parser.add_argument('-p', '--password', action="store", dest="secret", help="Database Name",
                    required=False)
parser.add_argument('-r', '--rds-instance', action="store", dest="instance", default="postgres", help="Engine",
                    required=False)
parser.add_argument('-e', '--engine', action="store", dest="engine", default="postgres", help="Engine",
                    required=False)

args = parser.parse_args()

fullprofile = args.profile
rds_engine = args.engine
user = args.rep_user
rds_instance = args.instance

if args.secret is not None:
    secret = args.secret

boto3.setup_default_session(profile_name=fullprofile)

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

def get_extensions(db, instance_data):
    conn = open_conn(db, instance_data)
    try:
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT extname FROM pg_extension WHERE extname similar to 'postgis%|pglogical|hstore'")
            result = cur.fetchall()
            ext_list = []
            for each_res in tuple(result):
                if each_res:
                    ext_list.append(each_res[0])
            cur.close()
            conn.close()
            return ext_list
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
        cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE "
                    "datname NOT IN ('{}',"
                    "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                    "'dba_%|%delete_me' ORDER BY pg_database_size(datname) "
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
    Recorro las instancias.
    Luego, guardo datos actuales.
    """
    i = instance['DBInstances'][0]

    final_dict = dict()
    final_dict[str(i['DBInstanceIdentifier'])] = dict()
    if is_master(i):
        dbs = list_databases(i)
        if dbs:
            for db in dbs:
                res = get_extensions(db, i)
                if res:
                    final_dict[str(i['DBInstanceIdentifier'])][db] = dict()
                    final_dict[str(i['DBInstanceIdentifier'])][db]['extensions'] = []
                    for each_ext in res:
                        final_dict[str(i['DBInstanceIdentifier'])][db]['extensions'].append(each_ext)
    logging.info(final_dict)


def get_rds_list():
    """
  Get list of all the DBs from RDS
  """
    rdsclient = boto3.client('rds')
    paginator = rdsclient.get_paginator('describe_db_instances')
    filter_list = []
    filter_list.append({'Name': 'engine', 'Values': [rds_engine]})
    pages = paginator.paginate(DBInstanceIdentifier=rds_instance, Filters=filter_list)

    for page in pages:
        process_instance(page)

def main():
    check_secret()
    get_rds_list()

if __name__ == '__main__':
    main()

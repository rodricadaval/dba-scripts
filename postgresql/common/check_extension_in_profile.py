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
from lib import dynamo
import logging

os.system('clear')

logging.basicConfig(filename='/tmp/extensions_in_profile.log', level=logging.INFO)


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


parser = argparse.ArgumentParser(description='Checking Databases with provided extensions')

parser.add_argument('--profile', action="store", dest="profile", help="xx-xx-east-1 | xx-xx-west-2 | xx1-xx-east-1 | "
                                                                      "xx1-xx-west-2 | xxx-xxx-east-1 | xxx-xxx-west-2 | "
                                                                      "xx1-xx-east-1 | xxxx-xx-west-2",
                    required=True, type=valid_profile)
parser.add_argument('-u', '--user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication")
parser.add_argument('-p', '--password', action="store", dest="secret", help="Database Name",
                    required=False)
parser.add_argument('-l', '--list-extensions', action="store", dest="db_extension_list", default="postgis",
                    help="Extensions list separated by comma", required=False)
parser.add_argument('-e', '--engine', action="store", dest="engine", default="postgres", help="Engine",
                    required=False)


args = parser.parse_args()

fullprofile = args.profile
rdsengine = args.engine
user = args.rep_user
if args.secret is not None:
    secret = args.secret
list_extensions = [x.strip() for x in args.db_extension_list.split(',')]

pprint("Profile: " + fullprofile)
boto3.setup_default_session(profile_name=fullprofile)
temp_dict = {}


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
        else:
            logging.error("Sin conectividad al RDS: {}. Detalle: {}".format(instance_data['Endpoint']['Address'], e))
        return None


def get_extensions(db, instance_data):
    conn = open_conn(db, instance_data)
    try:
        if conn:
            cur = conn.cursor()
            __qry_pg_extensions = """
                        SELECT
                        extname
                        FROM pg_extension 
                        WHERE extname IN ({});
                        """.format(str(list_extensions).replace("[", "").replace("]", ""))
            cur.execute(__qry_pg_extensions)
            result = cur.fetchall()
            cur.close()
            conn.close()
            if result:
                return result
            return None
    except psycopg2.errors.InsufficientPrivilege as e:
        if "permission denied for relation" in str(e):
            logging.info("No permissions in relation for host {} and user {}. Error: {}"
                         .format(instance_data['Endpoint']['Address'], user, e))
        return None
    return None


def list_databases(instance_data):
    pprint(instance_data)
    global secret
    my_connection = open_conn("postgres", instance_data)
    if my_connection:
        cur = my_connection.cursor()
        cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE datallowconn AND "
                    "datname NOT IN ('{}',"
                    "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                    "'dba_%|%delete_me|aws%' AND pg_database_size(datname) > 220000000 ORDER BY pg_database_size(datname) "
                    ") as bla;".format('rdsadmin', 'template1', 'template0', 'postgres'))
        result = cur.fetchall()
        cur.close()
        my_connection.close()
        return result[0][0]
    return None


def is_master(i):
    return "ReadReplicaSourceDBInstanceIdentifier" not in i.keys()


def process_instances(rdsInstances):
    """
    Recorro las instancias en búsqueda de replicas.
    Luego, guardo datos actuales.
    """
    for i in rdsInstances["DBInstances"]:
        if is_master(i):
            dbs = list_databases(i)
            if dbs:
                for db in dbs:
                    res = get_extensions(db, i)
                    if res:
                        if str(i['DBInstanceIdentifier']) not in temp_dict.keys():
                            temp_dict[i['DBInstanceIdentifier']] = dict()
                        if db not in temp_dict[i['DBInstanceIdentifier']].keys():
                            temp_dict[i['DBInstanceIdentifier']][db] = []
                        for each_ext in res:
                            temp_dict[i['DBInstanceIdentifier']][db].append(str(each_ext[0]))


def get_rds_list(rdsengine):
    """
  Get list of all the DBs from Postgres RDS
  """

    rdsclient = boto3.client('rds')
    paginator = rdsclient.get_paginator('describe_db_instances')
    filter_list = []
    filter_list.append({'Name': 'engine', 'Values': [rdsengine]})
    pages = paginator.paginate(Filters=filter_list)

    for page in pages:
        process_instances(page)

    if temp_dict:
        for each_rds in temp_dict.keys():
            for each_db in temp_dict[each_rds].keys():
                print("{},{},{},{}".format(fullprofile, each_rds, each_db, str(temp_dict[each_rds][each_db])))
    print()
    return pprint("Finished")


def main():
    check_secret()
    get_rds_list(rdsengine)


if __name__ == '__main__':
    main()

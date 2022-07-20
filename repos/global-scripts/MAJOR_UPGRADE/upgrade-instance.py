#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform auto-scaling to docdb replicas on nightshifts.
# Collect vital stats when needed
import datetime
import os
import json
import time
import traceback

import boto3
import psycopg2
from pprint import pprint
import sys
import argparse
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
import logging

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.basicConfig(
    format='%(asctime)-2s %(name)-2s %(levelname)-4s: %(message)s',
    level=logging.INFO, #Nivel de los eventos que se registran en el logger
)

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
rdsclient = boto3.client('rds')

__qry_1 = "DROP EXTENSION IF EXISTS pg_stat_statements;"
__qry_1 = "DROP VIEW IF EXISTS public.pg_stat_replication_all CASCADE;"
__qry_1 = "DROP VIEW IF EXISTS public.pg_stat_replication_summary CASCADE;"
__qry_1 = "DROP VIEW IF EXISTS public.pg_stat_replication_delay CASCADE;"
__qry_1 = "DROP VIEW IF EXISTS public.pg_stat_replication_slots_all CASCADE;"
__qry_1 = "DROP VIEW IF EXISTS public.pg_stat_replication_slots_summary CASCADE;"
__qry_1 = "DROP VIEW IF EXISTS public.pg_stat_statements_all CASCADE;"
__qry_1 = "DROP FUNCTION pg_stat_replication_all();"
__qry_1 = "DROP FUNCTION pg_stat_statements_all();"
__qry_1 = "DROP FUNCTION pg_stat_replication_slots_all();"


def get_major_version(data):
    return float(".".join(str(data).split(".", str(data).count("."))[:(str(data).count("."))]))


class Instance(object):
    def __init__(self, idata):
        self.endpoint = idata['Endpoint']['Address']
        self.region = self.endpoint.split(".")[2]
        self.name = idata['DBInstanceIdentifier']
        self.version = idata['EngineVersion']
        self.major_version = get_major_version(self.version)
        self.status = idata['DBInstanceStatus']


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


def block_if_extensions(db, instance_data):
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


def describe_upgrade_versions(inst, throttle=0.2):
    try:
        paginator = rdsclient.get_paginator('describe_db_engine_versions')
        pages = paginator.paginate(Engine=rds_engine, EngineVersion=inst.version)
        for page in pages:
            highest_version = inst.version
            for package in page['DBEngineVersions'][0]['ValidUpgradeTarget']:
                if get_major_version(package['EngineVersion']) > get_major_version(highest_version):
                    highest_version = package['EngineVersion']
            if get_major_version(highest_version) >= 12:
                logging.info("Version available to upgrade: {}".format(highest_version))
                logging.info("...OK")
            else:
                logging.info("No existe la posibilidad de upgrade a versiones >= 12. Pruebe primero realizando minor "
                             "version upgrade. Versión actual disponible: {}".format(highest_version))
                logging.info("...cannot continue")
                sys.exit(404)
    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle * 2)
            return describe_upgrade_versions(inst, throttle)
        else:
            pprint("Error getting aws instance data:" + str(e))
            sys.exit(1)


def refresh_describe(idata, throttle=0.2):
    """
    Get RDS data
    """
    try:
        rdsclient = boto3.client('docdb')
        result = rdsclient.describe_db_instances(
            DBInstanceIdentifier=idata['DBInstanceIdentifier'],
            Filters=[
                {
                    'Name': 'engine',
                    'Values': [rds_engine]
                }
            ]
        )

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle * 2)
            return refresh_describe(idata, throttle)
        else:
            pprint("Error getting aws instance data:" + str(e))
            sys.exit(1)
    else:
        return Instance(result['DBInstances'][0])


def block_if_unavailable(insta):
    if insta.status != "available":
        logging.info("Instance is in {} state. Cannot continue".format(insta.status))
        sys.exit(404)


def habilitar_conn(instance_data):
    try:
        my_connection = open_conn("postgres", instance_data)
        cur = my_connection.cursor()
        cur.execute("SELECT array_agg(quote_ident(datname)) FROM (SELECT datname FROM pg_database WHERE NOT datallowconn"
                    " AND datname NOT IN ('{}',"
                    "'{}','{}','{}') AND NOT datistemplate AND pg_catalog.pg_get_userbyid(datdba) NOT SIMILAR TO "
                    "'dba_%|%delete_me' ORDER BY pg_database_size(datname) "
                    "DESC) as bla;".format('rdsadmin', 'template1', 'template0', 'postgres'))
        result = cur.fetchall()
        cur.close()
        my_connection.close()
        if result[0][0]:
            dbs = result[0][0]
            for db in dbs:
                my_connection = open_conn(db, instance_data)
                cur.execute('ALTER DATABASE {} WITH ALLOW_CONNECTIONS true;'.format(db))
                cur.close()
                my_connection.close()
    except psycopg2.errors.InsufficientPrivilege as e:
        if "permission denied for relation" in str(e):
            logging.info("No permissions in database for host {} and user {}. Error: {}"
                         .format(instance_data['Endpoint']['Address'], user, e))
        sys.exit(499)
    except psycopg2.OperationalError as e:
        pprint("Sin conectividad al RDS. Detalle: {}".format(e))
        sys.exit(500)
    except Exception as e:
        logging.error("".format(traceback.print_exc()))
        sys.exit(404)


def check_data_types(db, instance_data):
    conn = open_conn(db, instance_data)
    try:
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT data_type FROM information_schema.columns where data_type ilike 'unknown';")
            result = cur.fetchall()
            ext_list = []
            for each_res in tuple(result):
                if each_res:
                    ext_list.append(each_res[0])
            cur.close()
            conn.close()
            return ext_list
    except Exception as e:
        logging.error("".format(e))
        sys.exit(404)


def process_instance(instance):
    """
    Recorro las instancias.
    Luego, guardo datos actuales.
    """
    i = instance['DBInstances'][0]

    inst = Instance(i)
    logging.info(inst.__dict__)

    if inst.major_version >= 12:
        logging.info("Your instance is already in version 12")
        sys.exit(1)

    describe_upgrade_versions(inst)
    inst = refresh_describe(i)

    block_if_unavailable(inst)

    start_time = datetime.datetime.now()
    logging.info("Start time: %s" % (start_time,))

    final_dict = dict()
    final_dict[str(i['DBInstanceIdentifier'])] = dict()
    final_dict[str(i['DBInstanceIdentifier'])]['data_types'] = []
    final_dict[str(i['DBInstanceIdentifier'])]['extensions'] = []

    if is_master(i):
        dbs = list_databases(i)
        if dbs:
            habilitar_conn(i)
            for db in dbs:
                res = check_data_types(db, i)
                if res:
                    final_dict[str(i['DBInstanceIdentifier'])]['extensions'].append(res)
                res = block_if_extensions(db, i)
                if res:
                    final_dict[str(i['DBInstanceIdentifier'])]['extensions'].append(res)
            if final_dict[str(i['DBInstanceIdentifier'])]['extensions']:
                logging.error("Existen tipos de datos que no permiten el upgrade")
                logging.error(final_dict[str(i['DBInstanceIdentifier'])]['extensions'])
                sys.exit(400)
            elif final_dict[str(i['DBInstanceIdentifier'])]['data_types']:
                logging.info("Existen extensiones que no permiten el upgrade. Deben mover esas dbs a otro RDS y "
                             "dropearlas")
                logging.info(final_dict[str(i['DBInstanceIdentifier'])]['data_types'])
                sys.exit(400)
    logging.info(final_dict)


def get_rds_list():
    """
    Get list of all the DBs from RDS
    """
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

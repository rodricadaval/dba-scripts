#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Migrate databases in one RDS to another.
# Reduce downtime to minimum

import os
import traceback
from ast import literal_eval

import requests
import json
from base64 import b64encode
import boto3
import datetime
import subprocess
import psycopg2
import pandas as pd
import re
from pprint import pprint
import sys
import argparse
import shelve
import time
import shutil
from shlex import quote as shlex_quote
from glob import glob
import decimal
import yaml
import dpath.util
import logging
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
from lib import dynamo_ms_variables
from pygments import highlight, lexers, formatters
from deepdiff import DeepDiff
from decimal import Decimal
from django.core.serializers.json import DjangoJSONEncoder

logging.basicConfig(filename='/tmp/migraciones.log', filemode='a', level=logging.DEBUG)

#os.system('clear')

parser = argparse.ArgumentParser(description='Migrate PostgreSQL Database Microservices to New RDS')

parser.add_argument('-c', '--country', action="store", dest="country",
                    help="Country -> [ar,br,cl,co,cr,ec,xx1,x1,xx1,dev,uy]", required=True)
parser.add_argument('-s', '--source', action="store", dest="source", help="AWS Instance Endpoint", required=True)
parser.add_argument('-t', '--target', action="store", dest="target", help="AWS Instance Endpoint", required=True)
parser.add_argument('-u', '--replication-user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication")
parser.add_argument('-d', '--database', action="store", dest="database", help="Database Name",
                    required=True)
parser.add_argument('-a', '--action', action="store", dest="action",
                    help="Action -> (start, migrate, abort-all, read-ms-vars, start-delayed-replication, fix-replication)",
                    required=True)
parser.add_argument('--no-initial-data', action="store_true", dest="no_initial_data", help="Do not sync existent data",
                    required=False)
parser.add_argument('-r', '--role', action="store", dest="source_role", help="Forced Role Name",
                    required=False)
parser.add_argument('-p', '--pass', action="store", dest="source_pass", help="Forced Role Pass",
                    required=False)
parser.add_argument('--replicate-after-full-dump-restore', action="store_true", dest="full_restore", help="Restauro "
                                                                                                          "toda la db"
                                                                                                          " y luego"
                                                                                                          " replico",
                    required=False)
parser.add_argument('--replicate-ddl-command', action="store", dest="ddl_command", help="DDL Statement",
                    required=False)
parser.add_argument('--resync-table', action="store", dest="sync_table",
                    help="Table to resync. Format:\"schema.table\"",
                    required=False)
parser.add_argument('--remove-table', action="store", dest="remove_table",
                    help="Table to remove from publication. Format:\"schema.table\"",
                    required=False)
parser.add_argument('--add-table', action="store", dest="add_table",
                    help="Table to add to replication set. Format:\"schema.table\"",
                    required=False)
parser.add_argument('--resync-all-tables', action="store_true", dest="resync_all_tables", required=False)

args = parser.parse_args()

country = args.country
source = args.source
target = args.target
database = args.database
rep_user = args.rep_user
action = args.action
if args.no_initial_data:
    no_initial_data = True
else:
    no_initial_data = False
if args.source_role:
    forced_source_role = args.source_role
if args.source_pass:
    forced_source_pass = args.source_pass
if args.ddl_command:
    ddl_command = args.ddl_command
if args.sync_table:
    sync_table = args.sync_table
if args.add_table:
    add_table = args.add_table
if args.remove_table:
    remove_table = args.remove_table


git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
if bool(glob(git_root + "/global-scripts/Migraciones_ECS/output*")):
    for f in glob(git_root + "/global-scripts/Migraciones_ECS/output*"):
        os.remove(f)
if bool(glob(git_root + "/global-scripts/Migraciones_ECS/errors*")):
    for f in glob(git_root + "/global-scripts/Migraciones_ECS/errors*"):
        os.remove(f)
out_file = "output.log." + country + "." + database + "." + str(time.time())
errors_file = "errors.log." + country + "." + database + "." + str(time.time())

try:
    source_endpoint = os.popen("nslookup " + source + " | grep Name | cut -d: -f2 | xargs").readline().strip()
    target_endpoint = os.popen("nslookup " + target + " | grep Name | cut -d: -f2 | xargs").readline().strip()

    if str(source_endpoint) == str(target_endpoint):
        pprint("Source and target cannot be the same RDS Endpoint")
        sys.exit(1)
except Exception as e:
    pprint("Error checking source and target endpoint difference" + str(e))
    sys.exit(1)

__qry_table_struct = """
SELECT table_schema, table_name, column_name, data_type 
FROM information_schema.columns
INNER JOIN pg_catalog.pg_namespace s ON s.nspname = table_schema
JOIN pg_catalog.pg_user u ON u.usesysid = s.nspowner
INNER JOIN pg_tables tbs ON tbs.schemaname = table_schema AND tbs.tablename = table_name 
WHERE table_schema NOT IN ('topology', 'tiger', 'pglogical', 'information_schema', 'pg_catalog', 'pglogical')
AND table_schema NOT LIKE 'pg_toast%' AND nspname NOT LIKE 'pg_temp_%'
AND table_name NOT SIMILAR TO 'spatial_ref_sys|pg_%'
AND u.usename NOT IN ('rdsadmin','rds_superuser')
AND tableowner NOT IN ('rdsadmin','rds_superuser')
ORDER BY table_schema, table_name, ordinal_position;
"""


def extractMsCredentials(ms_vars2):
    """
    Genera las variables del conector source
    """
    #pprint("extractMsCredentials")
    try:
        conf = dict()
        logging.info(str(json.dumps(ms_vars2)))

        for key in ['var_database', 'var_role', 'var_host', 'var_port', 'var_password']:
            if "var_password" not in ms_vars2.keys() and "var_password_url" in ms_vars2.keys() :
                ms_vars2["var_password"] = ms_vars2["var_password_url"]
            result = os.popen('python3 ' + git_root +
                              '/repos/python-scripts/dbacli variables get_variable --name '
                              + ms_vars2[key] + ' --country ' + country).read()
            if result.strip():
                conf[key.replace("var_", "")] = json.loads(result)['value']
            elif key == "var_password":
                result = os.popen('python3 ' + git_root +
                                  '/repos/python-scripts/dbacli variables get_variable --name '
                                  + ms_vars2[key + '_url'] + ' --country ' + country).read()
                conf[key.replace("var_", "")] = json.loads(result)['value']
            else:
                return dict()
        return conf
    except Exception as e:
        pprint("Error extracting ms credentials:" + str(e))
        sys.exit(1)


def check_dict_path(d, *indices):
    sentinel = object()
    for index in indices:
        d = d.get(index, sentinel)
        if d is sentinel:
            return False
    return True


def generateVarValues(vars):
    """
    Armo las vars
    """
    conf = dict()
    conf['var_host'] = vars['anansiblevar_database_host_var']
    conf['var_database'] = vars['anansiblevar_database_name_var']
    if "anansiblevar_database_password_var" in vars.keys():
        conf['var_password'] = vars['anansiblevar_database_password_var']
        if "_url" == vars['anansiblevar_database_password_var'][-4:]:
            conf['var_password'] = vars['anansiblevar_database_password_var']
    if "anansiblevar_database_password_var_url" in vars.keys():
        if "anansiblevar_database_password_var" in vars.keys():
            if "_url" != vars['anansiblevar_database_password_var'][-4:]:
                conf['var_password_url'] = vars['anansiblevar_database_password_var_url']
        else:
            conf['var_password'] = vars['anansiblevar_database_password_var_url']
    conf['var_port'] = vars['anansiblevar_database_port_var']
    conf['var_role'] = vars['anansiblevar_database_user_var']
    return conf

def pullMicroserviceCredentials(microservice_dict):
    """
    Genera el objeto del microservicio
    """
    #pprint("pullMicroserviceCredentials")
    try:
        conf_list = []
        home = os.path.expanduser("~")
        with open(home + '/repos/devops-requirements/ms-requirements/' + str(microservice_dict['ms']) + '.yml') as file:
            list_vars = yaml.load(file, Loader=yaml.FullLoader)
            if check_dict_path(list_vars, "resources", 'postgres'):
                for each_config in dpath.util.get(list_vars, "resources/postgres"):
                    if each_config['var_map']['anansiblevar_database_name_var'] == microservice_dict['database_var']:
                        role_vars = generateVarValues(each_config['var_map'])
                        if role_vars:
                            conf_list.append(role_vars)
            if check_dict_path(list_vars, "resources", 'monolith_database_access'):
                whole_data = dpath.util.get(list_vars, "resources/monolith_database_access")
                for skip_ms in whole_data.keys():
                    if whole_data[skip_ms]['var_map']['anansiblevar_database_name_var'] == microservice_dict['database_var']:
                        role_vars = generateVarValues(whole_data[skip_ms]['var_map'])
                        if role_vars:
                            conf_list.append(role_vars)
            if check_dict_path(list_vars, "requirements", 'new_postgres_database'):
                whole_data = dpath.util.get(list_vars, "requirements/new_postgres_database")
                for skip_ms in whole_data.keys():
                    if whole_data[skip_ms]['var_map']['anansiblevar_database_name_var'] == microservice_dict['database_var']:
                        role_vars = generateVarValues(whole_data[skip_ms]['var_map'])
                        if role_vars:
                            conf_list.append(role_vars)
            if check_dict_path(list_vars, "requirements", 'monolith_database_access'):
                whole_data = dpath.util.get(list_vars, "requirements/monolith_database_access")
                for skip_ms in whole_data.keys():
                    if whole_data[skip_ms]['var_map']['anansiblevar_database_name_var'] == microservice_dict['database_var']:
                        role_vars = generateVarValues(whole_data[skip_ms]['var_map'])
                        if role_vars:
                            conf_list.append(role_vars)
            if check_dict_path(list_vars, "resources", 'ms_database_access'):
                for each_data in dpath.util.get(list_vars, "resources/ms_database_access"):
                    if each_data['var_map']['anansiblevar_database_name_var'] == microservice_dict['database_var']:
                        role_vars = generateVarValues(each_data['var_map'])
                        if role_vars:
                            conf_list.append(role_vars)
        return conf_list
    except Exception as e:
        pprint("Error getting microservice vars from requirements:" + str(e))
        sys.exit(1)


def getUserPass():
    """
    Devuelve la password del usuario de replicacion
    """
    #pprint("getUserPass")
    if rep_user == "dba_test_service":
        try:
            i_dynamo = dynamo.DynamoClient('dev')
            var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
            return var['value']
        except NameError as e:
            print("No se encontraron variables del user en dynamo dev, busco en prod")
            i_dynamo = dynamo.DynamoClient('xx')
            var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
            return var['value']
        except Exception as e:
            print("No se encontraron variables del user en dynamo: {}".format(e))
            sys.exit(1)

def getDatabaseRegisteredVars():
    """
    Devuelve la password del usuario de replicacion
    """
    #pprint("getDatabaseRegisteredVars")
    try:
        variables = dict()
        file_path = "/tmp/shelvevariablesbydatabase" + "_" + country
        if os.path.isfile(file_path + ".db") or os.path.isfile(file_path):
            os.popen('sudo chmod 774 /tmp/shelvevariablesbydatabase' + '_' + '*').readline().strip()
            #os.popen('sudo chown ubuntu:adm /tmp/shelvevariablesbydatabase' + '_' + '*').readline().strip()
        d = shelve.open("/tmp/shelvevariablesbydatabase" + "_" + country, writeback=True)
        if str(database) in d.keys():
            for i in range(len(d[database])):
                if str(source_endpoint) in d[database][i].keys():
                    if time.time() - d[database][i]['last_check'] < 604800:
                        variables = d[database][i]
        d.close()
        return variables

    except Exception as e:
        pprint("Error opening shelve microservice vars cache file:" + str(e))
        sys.exit(1)


def save_data_in_dynamo_key(key, data):
    try:
        dynamomsvarscli = dynamo_ms_variables.DynamoMSVARSClient(country.lower())

        data = json.loads(json.dumps(data), parse_float=Decimal)
        dynamomsvarscli.update_vars(str(key), json.dumps(data, cls=DjangoJSONEncoder))
    except Exception as e:
        pprint("Error saving data in dynamo:" + str(e))


def getDatabaseRegisteredVarsDynamo():
    """
    Devuelve la password del usuario de replicacion
    """
    try:
        variables = dict()
        dynamomsvarscli = dynamo_ms_variables.DynamoMSVARSClient(country.lower())
        key = dynamomsvarscli.get_vars(str(database) + "_" + source_endpoint)
        if key:
            ms_data = json.loads(key['value'])
            for each_ms in ms_data:
                if time.time() - float(each_ms['last_check']) < 604800:
                    variables = each_ms
        return variables

    except Exception as e:
        pprint("Error getting microservice vars dynamo document:" + str(e))
        sys.exit(1)


def generateShelveAdminUsers():
    """
    Genera un shelve file con los admin users ubicados en pgpass.
    """
    if not os.path.isfile(git_root + "/.pgpass"):
        shutil.copy2("~/.pgpass", git_root + "/.pgpass")
    file = open(git_root + "/.pgpass", "r").read().splitlines()
    d = shelve.open("/tmp/svpass", writeback=True)
    for line in file:
        array = str(line).split(":")
        if array[3] != "postgres":
            d[array[3]] = array[4]

    d.close()


def openSourceConn():
    """
    Apertura de source connector
    """
    #pprint("openSourceConn")
    try:
        return psycopg2.connect(
            dbname=database, user=rep_user, port=5432, host=source, password=rep_user_pass)

    except Exception as e:
        pprint("Error connecting to source:" + str(e))
        sys.exit(1)

def openTargetPostgresConn():
    """
    Apertura de target postgres connector
    """
    #pprint("openTargetPostgresConn")
    try:
        return psycopg2.connect(
            dbname="postgres", user=rep_user, port=target_conf['port'], host=target_conf['host'],
            password=rep_user_pass)

    except Exception as e:
        pprint("Error connecting to postgres target database:" + str(e))
        sys.exit(1)


def get_role_source_configuration(role_data):
    try:
        conn_source_new = openSourceConn()
        conn_source_new.autocommit = True
        cur = conn_source_new.cursor()
        cur.execute("SELECT rolconnlimit, rolconfig FROM pg_roles WHERE rolname = '{}';".format(role_data['role']))
        result = cur.fetchone()
        if result is None:
            pprint("There are no roles with the name -> {}".format(role_data['role']))
            return None
        else:
            role_data['connection_limit'] = result[0]
            role_data['timeouts'] = result[1]
            if role_data['timeouts'] is None:
                role_data['timeouts'] = ['statement_timeout=60000', 'lock_timeout=30000',
                                         'idle_in_transaction_session_timeout=1200000']

            cur.close()
            conn_source_new.close()
            return role_data
    except Exception as e:
        pprint("Error generating role configuration:" + str(e))
        if "cur" in locals():
            cur.close()
            conn_source_new.close()
        closeConnections()
        sys.exit(1)


def createOrUpdateTargetMSRole():
    try:
        conn_postgres_target = openTargetPostgresConn()
        conn_postgres_target.autocommit = True
        cur_postgres_target = conn_postgres_target.cursor()
        cur_postgres_target.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = '" + target_conf['role'] + "';")
        if cur_postgres_target.fetchone() is None:
            print()
            print("Creating role in target...")
            print()
            cur_postgres_target.execute(
                "CREATE ROLE \"" + target_conf['role'] + "\" LOGIN PASSWORD '" + target_conf['password'] + "' INHERIT;")
        else:
            print()
            print("Altering role in target...")
            print()
            cur_postgres_target.execute(
                "ALTER ROLE \"" + target_conf['role'] + "\" WITH PASSWORD '" + target_conf['password'] + "';")

        each_ms = get_role_source_configuration(target_conf)
        cur_postgres_target.execute("ALTER ROLE \"{}\" CONNECTION LIMIT {};".format(target_conf['role'],
                                                                           each_ms['connection_limit']))
        for each_conf in each_ms['timeouts']:
            fields = each_conf.split("=")
            cur_postgres_target.execute("ALTER ROLE \"{}\" SET {}='{}';".format(target_conf['role'], fields[0],
                                                                       fields[1]))
        cur_postgres_target.execute("GRANT writeallaccess TO \"{}\";".format(target_conf['role']))
        cur_postgres_target.execute("GRANT \"{}\" TO postgres;".format(target_conf['role']))

        cur_postgres_target.close()
        conn_postgres_target.close()
    except Exception as e:
        pprint("Error in creating or updating target role:" + str(e))
        if "cur_postgres_target" in locals():
            cur_postgres_target.close()
        conn_postgres_target.close()
        if "conn_source" in globals():
            conn_source.close()
        if "conn_target" in globals():
            conn_target.close()
        sys.exit(1)


def getDatabaseOwner():
    """
    Devuelvo el owner de la base de datos
    """
    #pprint("getDatabaseOwner")
    try:
        conn_source_new = openSourceConn()
        conn_source_new.autocommit = True
        cursor = conn_source_new.cursor()
        cursor.execute("SELECT pg_catalog.pg_get_userbyid(d.datdba) FROM pg_catalog.pg_database d WHERE d.datname = "
                       "'" + database + "' ORDER BY 1;")
        row = cursor.fetchone()
        cursor.close()
        conn_source_new.close()
        return str(row[0])
    except Exception as e:
        pprint("Error getting database owner role:" + str(e))
        cursor.close()
        conn_source_new.close()
        sys.exit(1)


rep_user_pass = getUserPass()
owner_role = getDatabaseOwner()

def createTargetDatabase():
    pprint("createTargetDatabase")

    try:
        conn_postgres_target = openTargetPostgresConn()
        conn_postgres_target.autocommit = True
        cur_postgres_target = conn_postgres_target.cursor()

        if str(owner_role) != "postgres":
            createOrUpdateTargetMSRole()

        cur_postgres_target.execute(
            "CREATE DATABASE \"" + target_conf['database'] + "\" WITH OWNER \"" + target_conf['role'] + "\";")
        cur_postgres_target.close()
        conn_postgres_target.close()
    except Exception as e:
        pprint("Error in creating target database:" + str(e))
        if "cur_postgres_target" in locals():
            cur_postgres_target.close()
        conn_postgres_target.close()
        if "conn_source" in globals():
            conn_source.close()
        if "conn_target" in globals():
            conn_target.close()
        sys.exit(1)

def open_source_uncommited_conn():
    try:
        conn = psycopg2.connect(
            dbname=source_conf['database'], user=rep_user, port=source_conf['port'], host=source_conf['host'],
            password=rep_user_pass)
        conn.autocommit = False
        return conn
    except Exception as e:
        pprint("Error connecting to postgres source database:" + str(e))
        sys.exit(1)

def open_target_uncommited_conn():
    try:
        conn = psycopg2.connect(
            dbname=target_conf['database'], user=rep_user, port=target_conf['port'], host=target_conf['host'],
            password=rep_user_pass)
        conn.autocommit = False
        return conn
    except Exception as e:
        pprint("Error connecting to postgres target database:" + str(e))
        sys.exit(1)


def openTargetConn(timeout=False):
    """
    Apertura de target connector
    """
    try:
        if timeout:
            return psycopg2.connect(
                dbname=target_conf['database'], user=rep_user, port=target_conf['port'], host=target_conf['host'],
                password=rep_user_pass, options='-c statement_timeout=10000')
        else:
            return psycopg2.connect(
                dbname=target_conf['database'], user=rep_user, port=target_conf['port'], host=target_conf['host'],
                password=rep_user_pass)

    except Exception as e:
        if "database \"" + target_conf['database'] + "\" does not exist" in str(e):
            print("Database in target does not exist. Creating...")
            createTargetDatabase()
            return openTargetConn()
        else:
            pprint("Error connecting to target:" + str(e))
            sys.exit(1)


source_conf = dict()


def recreate_service_vars(variable):
    if "database" in variable or "db" in variable:
        role_cred = dict()
        role_cred_val = dict()
        for type in ["host", "password", "password_url", "user", "port", "database"]:
            if type != "database":
                role_cred[type] = variable.replace("database",type).replace("db",type)
            else:
                role_cred[type] = variable
            res = os.popen('python3 ' + git_root + '/repos/python-scripts/dbacli variables get_variable --name ' +
                           role_cred[type] + ' --country ' + country).read()
            if res is not None:
                res = res.strip().replace("\n","")
            if res:
                temp_dict = dict()
                temp_dict = json.loads(res)
                role_cred_val[type] = temp_dict['value']
        if "password_url" in role_cred_val.keys():
            if "password" not in role_cred_val.keys():
                role_cred_val["password"] = role_cred_val["password_url"]
            del role_cred_val["password_url"]
        if "user" in role_cred_val.keys():
            role_cred_val["role"] = role_cred_val["user"]
            del role_cred_val["user"]
        if "role" not in role_cred_val.keys() or "host" not in role_cred_val.keys() or "password" not in role_cred_val.keys() or "database" not in role_cred_val.keys():
            return None
        return role_cred_val
    return None


def generateSourceConfig():
    """
    Genera las variables del conector source y los microservicios que acceden
    """
    try:
        msvars = dict()
        msvars = getDatabaseRegisteredVarsDynamo()
        global source_conf

        if "forced_source_role" in globals() and "forced_source_pass" in globals():
            source_conf['database'] = database
            source_conf['role'] = forced_source_role
            source_conf['publication-name'] = database + '_publication'
            source_conf['host'] = source
            source_conf['port'] = 5432
            source_conf['password'] = forced_source_pass
        elif str(owner_role) == "postgres":
            source_conf['database'] = database
            source_conf['role'] = owner_role
            source_conf['publication-name'] = database + '_publication'
            source_conf['host'] = source
            source_conf['port'] = 5432
        if not msvars:
            d = []
            msvarslist = []
            microservices_vars = []
            microservices_vars = json.loads(os.popen('python3 ' + git_root +
                                                     '/repos/python-scripts/dbacli variables get_value --exact '
                                                     + database + ' --country ' + country).read())
            ids = [x['id'] for x in microservices_vars]
            ms_names = []
            for each_var in ids:
                home = os.path.expanduser("~")
                temp_result = os.popen('grep ' + each_var + ' ' + home +
                                       '/repos/devops-requirements/ms-requirements/* | cut -d: -f1 | sed "s/.yml//g" '
                                       '| awk -F/ \'{print $NF}\'').read().splitlines()#.readline().strip()
                if temp_result is not None and temp_result:
                    for i in range(0, len(temp_result)):
                        ms_w_var = dict()
                        ms_w_var['ms'] = temp_result[i]
                        ms_w_var['database_var'] = each_var
                        ms_names.append(ms_w_var)
                else:
                    non_ms_roles = recreate_service_vars(each_var)
                    if non_ms_roles:
                        service_endpoint = None
                        service_endpoint = os.popen(
                            "nslookup " + non_ms_roles['host'] + " | grep Name | cut -d: -f2 | xargs").readline().strip()
                        if str(service_endpoint) == str(source_endpoint):
                            msvarslist.append(non_ms_roles)
            ms_names_no_dup = pd.DataFrame(ms_names).drop_duplicates().to_dict('records')
            for each_ms in ms_names_no_dup:
                ms_variables_list = pullMicroserviceCredentials(each_ms)
                for ms_variables in ms_variables_list:
                    temp = extractMsCredentials(ms_variables)
                    if temp:
                        temp_endpoint = os.popen("nslookup " + temp['host'] + " | grep Name | cut -d: -f2 | xargs").readline().strip()
                        if str(temp_endpoint) == str(source_endpoint):
                            if str(temp['role']) == str(owner_role):
                                source_conf['database'] = temp['database']
                                source_conf['role'] = temp['role']
                                source_conf['publication-name'] = database + '_publication'
                                source_conf['host'] = temp['host']
                                source_conf['port'] = temp['port']
                                source_conf['password'] = temp['password']
                            msvarslist.append(temp)
            list_sources = []

            if d:
                temp2 = dict()
                temp2["data"] = msvarslist
                temp2['last_check'] = time.time()
                d.append(temp2)
            else:
                expectedResult = list(filter(lambda d: list(d)[0] in ["data"], d))
                if expectedResult:
                    for each_env in expectedResult:
                        each_env["data"] = msvarslist
                        each_env['last_check'] = time.time()
                        list_sources.append(each_env)
                else:
                    each_env = dict()
                    each_env["data"] = msvarslist
                    each_env['last_check'] = time.time()
                    list_sources = d
                    list_sources.append(each_env)
                d = list_sources
            save_data_in_dynamo_key(database + "_" + source_endpoint, d)
        else:
            if str(owner_role) != "postgres":
                res = msvars["data"]
                hasone = False
                for each_ms in res:
                    if str(each_ms['role']) == str(owner_role):
                        hasone = True
                        source_conf = each_ms
                        source_conf['publication-name'] = each_ms['database'] + '_publication'
                if not hasone:
                    if not res:
                        pprint("No se encontraron microservicios que utilicen la db {}".format(database))
                    else:
                        pprint("El owner de la db es: {} y no coincide con el microservicio. "
                               "Por favor corregir este error.".format(owner_role))
                    sys.exit(1)
    except Exception as e:
        pprint("Error generating source config and extra ms:" + str(e))
        traceback.print_exc()
        if "conn_source" in globals():
            conn_source.close()
        if "conn_target" in globals():
            conn_target.close()
        sys.exit(1)


def generateTargetConfig():
    """
    Genera las variables del conector target
    """
    #pprint("generateTargetConfig")
    try:
        conf = dict()
        conf['subscription-name'] = str(database).replace("-", "_") + "_subscription"
        conf['replication-slot'] = conf['subscription-name']
        conf['host'] = target
        return conf

    except Exception as e:
        pprint("Error generating source config:" + str(e))
        sys.exit(1)


def setServerVersion(conf, conn):
    """
    Setea la pg version en el conector especificado
    """
    #pprint("setServerVersion")
    cur = conn.cursor()
    cur.execute("SHOW server_version;")
    item = cur.fetchone()
    cur.close()
    conf['version'] = item[0]


def getSourceSchemas():
    """
    Obtiene los schemas de la db source y los guarda en un listado
    """
    try:
        cursor = conn_source.cursor()
        cursor.execute("select s.nspname as table_schema from pg_catalog.pg_namespace s join pg_catalog.pg_user u "
                       "on u.usesysid = s.nspowner WHERE nspname = 'public' and u.usename = 'rdsadmin';")
        rows = cursor.fetchall()
        if rows:
            cursor.execute("ALTER SCHEMA public OWNER TO \"{}\"".format(source_conf['role']))

        cursor.execute("select s.nspname as table_schema from pg_catalog.pg_namespace s join pg_catalog.pg_user u "
                       "on u.usesysid = s.nspowner where nspname not in "
                       "('information_schema', 'pg_catalog', 'pglogical', 'topology', 'tiger') and nspname "
                       "not like 'pg_toast%' and nspname not like 'pg_temp_%' and u.usename NOT IN ('rdsadmin', 'rds_superuser');")
        rows = cursor.fetchall()
        list_schemas = []
        for row in rows:
            list_schemas.append(str(row[0]))
        return list_schemas

    except Exception as e:
        pprint("Error in cursor getting source schemas:" + str(e))
        cursor.close()
        closeConnections()
        sys.exit(1)


def analyze_target():
    try:
        command = 'vacuumdb -h {} -U {} -d {} --analyze-in-stages --skip-locked --jobs 2'\
            .format(target_conf['host'], rep_user, target_conf['database'])
        out = open(out_file, "a+")
        error = open(errors_file, "a+")
        p = subprocess.Popen(command, stdout=out, stderr=error, shell=True, universal_newlines=True)
        p.wait(timeout=604000)
        out.close()
        error.close()
    except Exception as e:
        pprint("Error with subprocess popen command:" + str(e))
        closeConnections()
        sys.exit(499)


def checkMissingPks():
    #pprint("checkMissingPks")

    try:
        cur = conn_source.cursor()

        if source_conf['version'].split(".")[0] in ["11", "12", "10"]:
            cur.execute("select tab.table_schema || '.' || tab.table_name as table from information_schema.tables tab "
                        "left join information_schema.table_constraints tco on tab.table_schema = tco.table_schema and "
                        "tab.table_name = tco.table_name and tco.constraint_type = 'PRIMARY KEY' "
                        "left join pg_inherits inh ON inh.inhrelid::regclass::text = tab.table_name "
                        "where tab.table_type = 'BASE TABLE' and tab.table_schema not in ('pg_catalog', 'information_schema', 'pglogical') "
                        "and inh.inhrelid is null and tco.constraint_name is null order by tab.table_schema, tab.table_name;")
        else:
            cur.execute("SELECT n.nspname || '.' || c.relname as table FROM pg_catalog.pg_class c JOIN pg_namespace n "
                        "ON ( c.relnamespace = n.oid AND n.nspname NOT IN "
                        "('information_schema', 'pg_catalog', 'pglogical', 'tiger', 'topology') "
                        "AND c.relkind='r' ) "
                        "left join pg_inherits inh ON inh.inhrelid::regclass::text = c.relname "
                        "WHERE c.relhaspkey = false and inh.inhrelid is null;")
        rows = cur.fetchall()
        if rows:
            print("There tables without primary key. Cannot replicate. Please fix them and start again")
            for row in rows:
                print(str(row[0]))
            raise Exception("Cannot continue.")

    except Exception as e:
        print()
        pprint("Error in check all tables has primary keys:" + str(e))
        if "conn_source" in globals():
            conn_source.close()
        if "conn_target" in globals():
            conn_target.close()
        sys.exit(1)


generateSourceConfig()

if not source_conf:
    microdict = getDatabaseRegisteredVarsDynamo()
    if microdict:
        pretty_json = highlight(json.dumps(microdict["data"], indent=2, default=str), lexers.JsonLexer(),
                                  formatters.TerminalFormatter())
        if pretty_json:
            print("El owner {0} de la db {1} no pudo ser matcheado con un microservicio en el rds {2}, pero hay "
                  "variables que apuntan al host:".format(owner_role, database, source))
            print("{}".format(pretty_json))
            print("")
            print("Debes corregir el owner. Puede que el actual owner sea un ms del tipo external y eso no es correcto. "
                  "Chequear con:")
            print("python3 get_microservice_data.py -c {1} -d {0} -e postgres".format(database, country))
        raise Exception()
    else:
        print("No se encontraron dynamo vars que usen la db {0} en el rds {1}. El ms puede estar duplicado y "
              "operativo en otro host. Verifica mediante aws_dynamodb_anansiblevar_search.sh "
              "-c PAIS -k value -v {0}. Error: {2}".format(database, source, traceback.print_exc()))
        raise Exception()

target_conf = generateTargetConfig()
if "database" in source_conf.keys():
    target_conf['database'] = source_conf['database']
    target_conf['role'] = source_conf['role']
    target_conf['port'] = source_conf['port']
else:
    raise Exception("No se encontraron dynamo vars que usen la db {} en el rds {}. El ms puede estar duplicado y "
                    "operativo en otro host. Verifica mediante aws_dynamodb_anansiblevar_search.sh "
                    "-c PAIS -k value -v {}. Error: {}".format(database, source, database, traceback.print_exc()))
if str(owner_role) != "postgres":
    target_conf['password'] = source_conf['password']
conn_source = openSourceConn()
conn_source.autocommit = True
setServerVersion(source_conf, conn_source)
checkMissingPks()
conn_target = openTargetConn()
conn_target.autocommit = True
setServerVersion(target_conf, conn_target)
source_schemas = getSourceSchemas()
pprint("Source PG Version: " + source_conf['version'])
pprint("Target PG Version: " + target_conf['version'])
print()


def getDockerName(version):
    """
    Retorna el docker name segun la version de postgres del conector
    """
    return os.popen('sudo docker ps --format "{{.Names}}" | grep ' + version.split(".")[0]).read().splitlines()[0]


def startMigration():
    """
  Starts migration
  """
    if action == "start":
        adjustPermissions()
        createExtraRolesOnTarget(getDatabaseRegisteredVarsDynamo())
        dumpRestore()
        createPublication()
        alterTargetDatabaseOwner()
        createSubscriber()
        time.sleep(2)
        showReplicationStatus()
    elif action == "migrate":
        changes = check_non_changes()
        if changes:
            print("There are ddl differences between source and target.")
            diff = DeepDiff(changes['target'], changes['source'])
            colorful_json = highlight(json.dumps(diff, indent=2, default=str), lexers.JsonLexer(),
                                      formatters.TerminalFormatter())
            print(colorful_json)
            sys.exit(499)
        terminateSourceRoleConnections()
        waitTillCatchUp()
        # recreateExtraReplicationSlots()
        dropSubscription()
        dropPublication()
        alterTableAndSequenceOwnership()
        alterDatabaseOwnership()
        repairSequences()
        repairOrphans()
    elif action == "skip-prmissions":
        createPublication()
        alterTargetDatabaseOwner()
        createSubscriber()
    elif action == "create-publication":
        createPublication()
    elif action == "create-subscription":
        createSubscriber()
    elif action == "drop-publication":
        dropPublication()
    elif action == "drop-subscription":
        dropSubscription()
    elif action == "terminate-connections":
        terminateSourceRoleConnections()
    elif action == "repair-ownership":
        alterTableAndSequenceOwnership()
        alterDatabaseOwnership()
    elif action == "repair-sequences":
        repairSequences()
        repairOrphans()
    elif action == "repair-orphan-sequences":
        repairOrphans()
    elif action == "abort-all":
        dropExtraReplicationSlots()
        dropSubscription()
        dropPublication()
        roolbackToBeginning()
    elif action == "recreate-target-database":
        roolbackToBeginning()
    elif action == "show-replication-delay":
        waitTillCatchUp()
    elif action == "read-ms-vars":
        print("Source Conf:")
        pprint(source_conf)
        print()
        print("Target Conf:")
        pprint(target_conf)
    elif action == "drop-target-database":
        dropTargetDatabase()
    elif action == "show-replication-status":
        showReplicationStatus(check_non_changes())
    elif action == "check-all-roles":
        microdict = getDatabaseRegisteredVarsDynamo()
        if microdict:
            pprint(microdict["data"])
        else:
            print("There are no extra roles to create.")
    elif action == "create-extra-roles":
        createExtraRolesOnTarget(getDatabaseRegisteredVarsDynamo())
    elif action == "dev-create-nominal-roles":
        createNominalRolesOnTarget()
    elif action == "repair-ms-target-role":
        createOrUpdateTargetMSRole()
    elif action == "recreate-extra-slots":
        recreateExtraReplicationSlots()
    elif action == "drop-extra-slots":
        dropExtraReplicationSlots()
    elif action == "move-schema-only":
        dumpRestore("schema")
    elif action == "dump-restore-schema":
        dumpRestore("schema")
    elif action == "migrate-with-full-dump":
        adjustPermissions()
        createExtraRolesOnTarget(getDatabaseRegisteredVarsDynamo())
        conn_limit = get_source_role_connections()
        terminateSourceRoleConnections()
        dumpRestore("full")
        set_role_connections_in_target(conn_limit)
        alterTableAndSequenceOwnership()
        alterDatabaseOwnership()
        repairSequences()
    elif action == "full-dump-restore":
        adjustPermissions()
        createExtraRolesOnTarget(getDatabaseRegisteredVarsDynamo())
        conn_limit = get_source_role_connections()
        dumpRestore("full")
        set_role_connections_in_target(conn_limit)
    elif action == "enable-subscription":
        changeSubscriberState("enable")
        showReplicationStatus(check_non_changes())
    elif action == "fix-replication":
        if args.add_table:
            add_table_to_replication_set()
        if args.remove_table:
            remove_table_from_replication()
        if args.ddl_command and args.sync_table:
            replicate_ddl_command()
            resync_table()
        if args.ddl_command and not args.sync_table:
            replicate_ddl_command()
        if args.sync_table and not args.ddl_command:
            resync_table()
        if args.resync_all_tables:
            resync_all_tables()
    elif action == "disable-subscription":
        changeSubscriberState("disable")
        showReplicationStatus(check_non_changes())
    elif action == "show-subscription-table-status":
        show_subscription_table_status()
    elif action == "start-delayed-replication":
        adjustPermissions()
        createExtraRolesOnTarget(getDatabaseRegisteredVarsDynamo())
        dumpRestore("schema")
        createPublication()
        alterTargetDatabaseOwner()
        createSubscriber(True)
        dumpRestore("data")
        changeSubscriberState("enable")
        time.sleep(2)
        showReplicationStatus()
    elif action == "create-stopped-replication":
        adjustPermissions()
        createExtraRolesOnTarget(getDatabaseRegisteredVarsDynamo())
        dumpRestore("schema")
        createPublication()
        alterTargetDatabaseOwner()
        createSubscriber(True)
    elif action == "analyze-target":
        analyze_target()
        print("Finished analyzing")
    else:
        print("There are no actions with this name")


def adjustPermissions():
    """
    Prepara y aplica los permisos en source y target
    """
    #pprint("adjustPermissions")
    cur_source = conn_source.cursor()
    cur_target = conn_target.cursor()
    fixPermissions()
    grantReplication(cur_source)
    grantReplication(cur_target)
    cur_source.close()
    cur_target.close()


def fixPermissions():
    """
    Envia los files al docker correspondiente y corre los scripts
    """
    pprint("fixPermissions")

    sendFilesOnSource()
    executeGrantFilesOnSource()
    sendFilesOnTarget()
    executeGrantFilesOnTarget()


def sendFilesOnTarget():
    """
    Copia los files desde el repo hacia el docker correspondiente. Segun connector pg version
    """
    pprint("sendFilesOnTarget")
    try:
        value = os.popen(
            "sudo docker inspect " + getDockerName(target_conf['version']) + " -f '{{.Id}}'").read().splitlines()[0]
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_admin_roles.sql " + value + ":/tmp/pg_fix_admin_roles.sql")
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_grant_roles_new_rds.sql " + value + ":/tmp/pg_fix_grant_roles_new_rds.sql")
    except Exception as e:
        pprint("Error copying files to docker:" + str(e))
        closeConnections()
        sys.exit(1)


def executeGrantFilesOnTarget():
    """
    Ejecuta los files enviados previamente al docker
    """
    pprint("executeGrantFilesOnTarget")

    try:
        logCommand("sudo docker exec -it " + getDockerName(
            target_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                       'host'] + " -U " + rep_user + " -d " + target_conf[
                       'database'] + " -f " + "/tmp/pg_fix_admin_roles.sql\"")
        logCommand("sudo docker exec -it " + getDockerName(
            target_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                       'host'] + " -U " + rep_user + " -d " + target_conf[
                       'database'] + " -f " + "/tmp/pg_fix_grant_roles_new_rds.sql\"")
    except Exception as e:
        pprint("Error executing exported files in target:" + str(e))
        closeConnections()
        sys.exit(1)


def sendFilesOnSource():
    """
    Copia los files desde el repo hacia el docker correspondiente. Segun connector pg version
    """
    pprint("sendFilesOnSource")
    out = open(out_file, "w")
    error = open(errors_file, "w")
    out.close()
    error.close()
    try:
        value = os.popen(
            "sudo docker inspect " + getDockerName(source_conf['version']) + " -f '{{.Id}}'").read().splitlines()[0]
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_admin_roles.sql " + value + ":/tmp/pg_fix_admin_roles.sql")
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_grant_roles_new_rds.sql " + value + ":/tmp/pg_fix_grant_roles_new_rds.sql")
    except Exception as e:
        pprint("Error copying files to docker:" + str(e))
        closeConnections()
        sys.exit(1)


def executeGrantFilesOnSource():
    """
    Ejecuta los files enviados previamente al docker
    """
    pprint("executeGrantFilesOnSource")
    try:
        logCommand("sudo docker exec -it " + getDockerName(
            source_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + source_conf[
                       'host'] + " -U " + rep_user + " -d " + source_conf[
                       'database'] + " -f " + "/tmp/pg_fix_admin_roles.sql\"")
        logCommand("sudo docker exec -it " + getDockerName(
            source_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + source_conf[
                       'host'] + " -U " + rep_user + " -d " + source_conf[
                       'database'] + " -f " + "/tmp/pg_fix_grant_roles_new_rds.sql\"")
    except Exception as e:
        pprint("Error executing exported files in source:" + str(e))
        closeConnections()
        sys.exit(1)

def get_source_role_connections():
    try:
        cur_source = conn_source.cursor()
        cur_source.execute("SELECT rolconnlimit FROM pg_roles WHERE rolname = '{}';".format(source_conf['role']))
        result = cur_source.fetchone()[0]
        pprint(result)
        cur_source.close()
        return result
    except Exception as e:
        pprint("Error getting source role connection limit:" + str(e))


def set_role_connections_in_target(connlimit):
    try:
        cur_target = conn_target.cursor()
        cur_target.execute("ALTER ROLE \"{}\" WITH CONNECTION LIMIT {};".format(source_conf['role'], connlimit))
        cur_target.close()
    except Exception as e:
        pprint("Error setting target role connection limit:" + str(e))

def check_non_changes():
    try:
        cur_source = conn_source.cursor()
        cur_source.execute(__qry_table_struct)
        struct_source = cur_source.fetchall()
        if struct_source:
            source_dict = {}
            for each_tup in struct_source:
                if str(each_tup[0]) not in source_dict.keys():
                    source_dict[str(each_tup[0])] = {}
                if each_tup[1] not in source_dict[str(each_tup[0])].keys():
                    source_dict[str(each_tup[0])][each_tup[1]] = {}
                source_dict[str(each_tup[0])][each_tup[1]][each_tup[2]] = each_tup[3]
        else:
            raise Exception("No se registran tablas en source")

        cur_target = conn_target.cursor()
        cur_target.execute(__qry_table_struct)
        struct_target = cur_target.fetchall()
        if struct_target:
            target_dict = {}
            for each_tup in struct_target:
                if str(each_tup[0]) not in target_dict.keys():
                    target_dict[str(each_tup[0])] = {}
                if each_tup[1] not in target_dict[str(each_tup[0])].keys():
                    target_dict[str(each_tup[0])][each_tup[1]] = {}
                target_dict[str(each_tup[0])][each_tup[1]][each_tup[2]] = each_tup[3]
        else:
            raise Exception("No se registran tablas en target")
        cur_target.close()
        cur_source.close()
        if target_dict == source_dict:
            return None
        else:
            return {"source": source_dict, "target": target_dict}
    except Exception as e:
        pprint("Error comparing target and source ddls:" + str(e))
        sys.exit(499)

def dumpRestore(type="schema"):
    """
    Aplica dump en source y restore en target. Se conecta al docker del source
    """
    try:
        pprint("dumpRestore module. Adjusting permissions prior dump")
        source_uncomm_conn = open_source_uncommited_conn()
        cur_uncomm_source = source_uncomm_conn.cursor()
        cur_source = conn_source.cursor()
        with conn_target.cursor() as cur_target_temp:
            cur_target_temp.execute("CREATE SCHEMA IF NOT EXISTS pglogical;")
            cur_target_temp.execute("ALTER ROLE fivetran_appl SET statement_timeout = 0;")
        if str(owner_role) != str(rep_user):
            cur_source.execute(
                "GRANT \"{}\" TO \"{}\";".format(owner_role, rep_user))
        if str(owner_role) != "postgres":
            cur_source.execute(
                "GRANT \"{}\" TO \"{}\";".format(owner_role, "postgres"))
        for schema in source_schemas:
            cur_uncomm_source.close()
            source_uncomm_conn.close()
            source_uncomm_conn = open_source_uncommited_conn()
            cur_uncomm_source = source_uncomm_conn.cursor()
            cur_uncomm_source.execute("SET ROLE \"{}\";".format(owner_role))
            cur_target = conn_target.cursor()
            if str(schema) != "public":
                cur_target.execute("CREATE SCHEMA IF NOT EXISTS \"" + schema + "\";")
            cur_target.execute("GRANT ALL ON ALL TABLES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
            cur_target.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
            try:
                cur_uncomm_source.execute("GRANT ALL ON ALL TABLES IN SCHEMA \"{}\" TO \"{}\";"
                                          .format(schema, "postgres"))
                cur_uncomm_source.execute(
                    "GRANT ALL ON ALL TABLES IN SCHEMA \"{}\" TO \"{}\";".format(schema, rep_user))
                cur_uncomm_source.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{}\" TO \"{}\";"
                                          .format(schema, "postgres"))
                cur_uncomm_source.execute(
                    "GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{}\" TO \"{}\";".format(schema, rep_user))
                cur_uncomm_source.execute("GRANT ALL ON SCHEMA \"{}\" TO \"{}\";".format(schema, "postgres"))
                cur_uncomm_source.execute("GRANT ALL ON SCHEMA \"{}\" TO \"{}\";".format(schema, rep_user))
                source_uncomm_conn.commit()
            except psycopg2.errors.InsufficientPrivilege as e:
                pprint("Hubo un error de permisos con usuario postgres. Tratando de fixearlo")
                cur_source = conn_source.cursor()
                cur_source.execute("GRANT ALL ON ALL TABLES IN SCHEMA \"{}\" TO \"{}\";"
                                   .format(schema, "postgres"))
                cur_source.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{}\" TO \"{}\";"
                                   .format(schema, "postgres"))
            cur_source.execute(
                "ALTER SCHEMA \"{}\" OWNER TO \"{}\";".format(schema, source_conf['role']))
            cur_source.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{}' "
                               "AND tableowner = '{}';".format(schema, rep_user))
            result = cur_source.fetchall()
            if result:
                for res in result:
                    cur_source.execute("ALTER TABLE \"{}\".\"{}\" OWNER TO \"{}\";".format(schema, str(res[0]),
                                                                                           source_conf['role']))
        cur_uncomm_source.close()
        source_uncomm_conn.close()
        cur_target.close()
        cur_source.close()

        if type == "schema":
            add_cmd = "-s"
        elif type == "data":
            add_cmd = "-a"
        elif type == "full":
            add_cmd = ""

        print("Performing pg_dump | pg_restore")
        logCommand(
            "sudo docker exec -it " + getDockerName(
                source_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " pg_dump -h " + source_conf[
                'host'] + " " + add_cmd + " --no-owner -U " + rep_user + " -N topology -N tiger -N pglogical -T public.spatial_ref_sys --role=postgres -d " +
            source_conf[
                'database'] + " | PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                'host'] + " -U " + rep_user + " -d " + target_conf['database'] + "\"")
    except psycopg2.errors.InsufficientPrivilege as e:
        pprint("Error: {}".format(traceback.print_exc()))
    except Exception as e:
        if "schema" not in str(e) and "already exists" not in str(e):
            pprint("Error in {}-dump-restore segment: {}".format(type, str(e)))
            closeConnections()
            sys.exit(1)
        else:
            pprint("El esquema ya existe. Continuo:" + str(e))

def logCommand(command):
    """
    Ejecucion de bash commands
    """
    try:
        out = open(out_file, "a+")
        error = open(errors_file, "a+")
        p = subprocess.Popen(command, stdout=out, stderr=error, shell=True, universal_newlines=True)
        p.wait(timeout=604000)
        out.close()
        error.close()
    except Exception as e:
        pprint("Error with subprocess popen command:" + str(e))
        closeConnections()
        sys.exit(1)


def grantReplication(cursor):
    """
    Aplica los grant para permitir la replicacion al replication user especificado
    """
    try:
        pprint("grantReplication")

        cursor.execute("GRANT rds_replication TO \"{}\";".format(rep_user))
        cursor.execute("GRANT postgres TO \"{}\";".format(rep_user))
        cursor.execute("GRANT rds_superuser TO \"{}\";".format(rep_user))
        cursor.execute("ALTER ROLE \"{}\" SET statement_timeout = 0;".format(rep_user))
        cursor.execute("ALTER ROLE \"{}\" SET lock_timeout = 0;".format(rep_user))
    except Exception as e:
        pprint("Error in cursor granting replication permissions:" + str(e))
        cursor.close()
        closeConnections()
        sys.exit(1)


def waitTillCatchUp():
    pprint("waitTillCatchUp")

    try:
        finished = False
        cur_source = conn_source.cursor()
        target_conf['replication-slot'] = getReplicationSlotName()
        while not finished:
            if source_conf['version'].split(".")[0] == "9":
                cur_source.execute("SELECT slot_name, confirmed_flush_lsn as flushed, pg_current_xlog_location(), "
                                   "(pg_current_xlog_location() - confirmed_flush_lsn) AS lsn_distance FROM "
                                   "pg_catalog.pg_replication_slots WHERE slot_name = '" + target_conf['replication-slot'] + "';")
            elif source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
                cur_source.execute("SELECT slot_name, confirmed_flush_lsn as flushed, pg_current_wal_lsn(), "
                                   "(pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance FROM "
                                   "pg_catalog.pg_replication_slots WHERE slot_name = '" + target_conf[
                                       'replication-slot']
                                   + "';")

            delay = cur_source.fetchone()[3]
            if delay > 100:
                print("Replication remaining wals to catch up: " + str(delay))
                if "prior_delay" in locals():
                    estimatedEta(prior_delay, delay)
                time.sleep(1)
                prior_delay = delay
            else:
                print("Replication up to date: " + str(delay))
                finished = True

        cur_source.close()
    except Exception as e:
        pprint("Error in getting slot replication distance:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def estimatedEta(before, now):
    try:
        result = 9999
        result = round(decimal.Decimal(before) / (decimal.Decimal(before) - decimal.Decimal(now)))
    except Exception as e:
        result = 9999
    finally:
        if "result" in locals():
            if result < 0:
                print("ETA -> Close to end")
            else:
                print("ETA -> " + str(result) + " seconds")
        else:
            return estimatedEta(before, now)


def createPublication():
    pprint("createPublication")
    try:
        cur_source = conn_source.cursor()
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_source.execute("CREATE PUBLICATION \"" + source_conf['publication-name'] + "\" FOR ALL TABLES;")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_source.execute("CREATE EXTENSION IF NOT EXISTS pglogical;")
            cur_source.execute("GRANT USAGE ON SCHEMA pglogical TO {};".format("writeallaccess"))
            cur_source.execute("GRANT ALL ON SCHEMA pglogical TO \"{}\";".format(source_conf['role']))

            cur_source.execute("SELECT pglogical.create_node(node_name := '" + source_conf['publication-name']
                               + "', dsn := 'host=" + source_conf['host']
                               + " port=" + str(source_conf['port']) + " user=" + rep_user + " password=" + rep_user_pass
                               + " dbname=" + source_conf['database'] + "');")
            cur_source.execute(
                "SELECT pglogical.replication_set_add_all_tables('default', ARRAY" + str(source_schemas) + ");")
            cur_source.execute("SELECT 1 FROM pg_tables WHERE tablename = 'spatial_ref_sys';")
            if cur_source.fetchone():
                cur_source.execute("SELECT pglogical.replication_set_remove_table('default', 'public.spatial_ref_sys');")
            cur_source.execute(
                "SELECT pglogical.replication_set_add_all_sequences('default', ARRAY" + str(source_schemas) + ");")
        cur_source.close()
    except Exception as e:
        pprint("Error in cursor creating publication:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def changeSubscriberState(state):
    pprint("changeSubscriberState")
    try:
        cur_target = conn_target.cursor()
        if state == "enable":
            cur_target.execute("SELECT pglogical.alter_subscription_enable(subscription_name := '"
                              + target_conf['subscription-name'] + "', immediate := true);")
        elif state == "disable":
            cur_target.execute("SELECT pglogical.alter_subscription_disable(subscription_name := '"
                              + target_conf['subscription-name'] + "', immediate := true);")
        cur_target.close()
    except Exception as e:
        pprint("Error in cursor changing subscription state:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def replicate_ddl_command():
    pprint("replicate_ddl")
    try:
        if args.ddl_command:
            cur_target = conn_target.cursor()
            cur_target.execute("SELECT pglogical.replicate_ddl_command('{}');".format(ddl_command))
            cur_target.close()
        else:
            raise Exception("No ha especificado ningun comando con el flag --replicate-ddl-command")
    except Exception as e:
        pprint("Error in cursor replicating ddl command:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def add_table_to_replication_set():
    pprint("add_table_to_replication")
    try:
        if args.add_table:
            cur_source = conn_source.cursor()
            cur_source.execute("SELECT 1 FROM pg_tables WHERE tablename = '{}';".format(add_table))
            if cur_source.fetchone():
                if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
                    cur_source.execute(
                        "ALTER PUBLICATION \"" + source_conf['publication-name'] + "\" ADD TABLE \"{}\";".format(add_table))
                elif source_conf['version'].split(".")[0] in ["9"]:
                    cur_source.execute(
                        "SELECT pglogical.replication_set_add_table('default', '{}', 'true');".format(str(add_table)))
            cur_source.close()
        else:
            raise Exception("No ha especificado ninguna tabla con el flag --add-table")
    except Exception as e:
        pprint("Error in cursor adding new table:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def remove_table_from_replication():
    pprint("remove_table_from_replication")
    try:
        if args.remove_table:
            cur_source = conn_source.cursor()
            cur_source.execute("SELECT 1 FROM pg_tables WHERE tablename = '{}';".format(remove_table))
            if cur_source.fetchone():
                if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
                    cur_source.execute("ALTER PUBLICATION \"" + source_conf['publication-name'] + "\" DROP TABLE \"{}\";"
                                       .format(remove_table))
                elif source_conf['version'].split(".")[0] in ["9"]:
                    cur_source.execute("SELECT pglogical.replication_set_remove_table('default', '{}');"
                                        .format(remove_table))
            cur_source.close()
        else:
            raise Exception("No ha especificado ninguna tabla con el flag --remove-table")
    except Exception as e:
        pprint("Error in cursor removing table from publication:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def resync_table():
    pprint("resync_table")
    try:
        if args.sync_table:
            cur_target = conn_target.cursor()
            cur_target.execute("SELECT pglogical.alter_subscription_resynchronize_table('{}','{}');"
                               .format(target_conf['subscription-name'], sync_table))
            cur_target.close()
        else:
            raise Exception("No ha especificado ninguna tabla con el flag --resync-table")
    except Exception as e:
        pprint("Error in cursor syncing table:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def resync_all_tables():
    pprint("resync_all_tables")
    try:
        if args.resync_all_tables:
            cur_target = conn_target.cursor()
            cur_target.execute("SELECT pglogical.alter_subscription_synchronize('{}','true');"
                               .format(target_conf['subscription-name']))
            cur_target.close()
        else:
            raise Exception("Error en resync all tables. El flag --resync-all-tables no fue utilizado.")
    except Exception as e:
        pprint("Error in cursor resyncing all tables:" + str(e))
        traceback.print_exc
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def show_subscription_table_status():
    pprint("show_subscription_table_status")

    try:
        cur_target = conn_target.cursor()
        if source_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute(
                "SELECT nspname || '.' || relname FROM pglogical.tables;")
            rows = cur_target.fetchall()
            for row in rows:
                cur_target.execute("SELECT pglogical.show_subscription_table('{}','{}')"
                                   .format(target_conf['subscription-name'], str(row[0])))
                result = cur_target.fetchone()
                if result:
                    if "replicating" not in str(result):
                        print("{}".format(result[0]))
        cur_target.close()
    except Exception as e:
        pprint("Error checking table subscription status:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def createSubscriber(disabled=False):
    pprint("createSubscriber")
    try:
        cur_target = conn_target.cursor()
        for schema in source_schemas:
            cur_target.execute("GRANT ALL ON ALL TABLES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
            cur_target.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            if not no_initial_data:
                cur_target.execute(
                    "CREATE SUBSCRIPTION \"" + target_conf['subscription-name'] + "\" CONNECTION 'host=" + source_conf['host'] +
                    " port=5432 dbname=" + source_conf['database'] + " user=" + rep_user + " password=" + rep_user_pass +
                    "' PUBLICATION \"" + source_conf['publication-name'] + "\";")
            else:
                cur_target.execute(
                    "CREATE SUBSCRIPTION \"" + target_conf['subscription-name'] + "\" CONNECTION 'host="
                    + source_conf['host'] + " port=5432 dbname=" + source_conf['database']
                    + " user=" + rep_user + " password=" + rep_user_pass + "' PUBLICATION \""
                    + source_conf['publication-name'] + "\" WITH (copy_data = false);")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute("CREATE EXTENSION IF NOT EXISTS pglogical WITH SCHEMA pglogical;")
            cur_target.execute("GRANT ALL ON SCHEMA pglogical TO \"{}\";".format(target_conf['role']))
            cur_target.execute("SELECT pglogical.create_node(node_name := '" + target_conf['subscription-name']
                               + "', dsn := 'host=" + target_conf['host'] + " port=" + str(target_conf['port'])
                               + " user=" + rep_user + " password=" + rep_user_pass + " dbname="
                               + target_conf['database'] + "');")
            cur_target.execute("GRANT USAGE ON SCHEMA pglogical TO {};".format("writeallaccess"))
            if disabled:
                cur_target.execute("SELECT pglogical.create_subscription(subscription_name := '"
                                    + target_conf['subscription-name'] + "', provider_dsn := 'host="
                                    + source_conf['host'] + " port=" + str(source_conf['port']) + " user=" + rep_user
                                    + " password=" + rep_user_pass + " dbname=" + source_conf['database']
                                    + "', synchronize_data := false);")
                cur_target.execute("SELECT pglogical.alter_subscription_disable(subscription_name := '"
                                    + target_conf['subscription-name'] + "', immediate := true);")
            else:
                if not no_initial_data:
                    cur_target.execute("SELECT pglogical.create_subscription(subscription_name := '"
                                           + target_conf['subscription-name'] + "', provider_dsn := 'host="
                                           + source_conf['host'] + " port=" + str(source_conf['port']) + " user=" + rep_user
                                           + " password=" + rep_user_pass + " dbname=" + source_conf['database'] + "');")
                else:
                    cur_target.execute("SELECT pglogical.create_subscription(subscription_name := '"
                                       + target_conf['subscription-name'] + "', provider_dsn := 'host="
                                       + source_conf['host'] + " port=" + str(source_conf['port']) + " user=" + rep_user
                                       + " password=" + rep_user_pass + " dbname=" + source_conf['database']
                                       + "', synchronize_data := false);")
        cur_target.close()
    except Exception as e:
        pprint("Error in cursor creating subscription:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def alterTargetDatabaseOwner():
    pprint("alterTargetDatabaseOwner")
    try:
        cur_target = conn_target.cursor()
        cur_target.execute("ALTER DATABASE \"" + target_conf['database'] + "\" OWNER TO \"" + rep_user + "\";")
        cur_target.close()
    except Exception as e:
        pprint("Error in cursor altering database ownership to rep user:" + str(e))
        cur_target.close()
        closeConnections()
        sys.exit(1)


def dropPublication():
    pprint("dropPublication")
    try:
        cur_source = conn_source.cursor()
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_source.execute("DROP PUBLICATION \"" + source_conf['publication-name'] + "\";")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_source.execute("SELECT pglogical.drop_node('" + source_conf['publication-name'] + "');")
        cur_source.close()
    except Exception as e:
        pprint("Error in cursor dropping publication and subscription:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def showReplicationStatus(changes=None):
    pprint("showReplicationStatus")

    try:
        cur_target = conn_target.cursor()
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_target.execute("SELECT * FROM pg_subscription sub INNER JOIN pg_stat_subscription stat ON stat.subid = "
                               "sub.oid WHERE sub.subname = '" + target_conf['subscription-name'] + "';")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute("SELECT pglogical.show_subscription_status('" + target_conf['subscription-name'] + "');")
        result = cur_target.fetchone()
        if result is None:
            print("There is no replication ongoing within this microservice and target host")
        else:
            if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
                print()
                print("Subscriber Node: " + str(result[2]))
                if changes:
                    print("State: " + str(result[4]) + " with ddl changes. Need to abort or repair.")
                else:
                    print("State: " + str(result[4]))
                print("Publication Node: " + str(result[8]))
                print("Synchronous Commit: " + str(result[7]))
                print("Slot Name: " + result[6])
                print("Received lsn: " + result[13])
                print("last_msg_send_time: " + str(result[14]))
                print("last_msg_receipt_time: " + str(result[15]))
            else:
                res = str(result[0]).split(",")
                print()
                print("Subscriber Node: " + res[0].replace("(", ""))
                if changes:
                    print("State: " + res[1] + " with ddl changes. Need to abort or repair.")
                else:
                    print("State: " + res[1])
                print("Publication Node: " + res[2])
                print("Slot Name: " + res[4])
                print("Tables: " + res[8].replace(")", ""))
        if changes:
            print("")
            print("There are ddl differences between source and target.")
            diff = DeepDiff(changes['target'], changes['source'])
            colorful_json = highlight(json.dumps(diff, indent=2, default=str), lexers.JsonLexer(),
                                      formatters.TerminalFormatter())
            print(colorful_json)
        cur_target.close()
    except Exception as e:
        pprint("Error checking replication status:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def getReplicationSlotName():
    try:
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            return target_conf['replication-slot']
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_target = conn_target.cursor()
            cur_target.execute("SELECT pglogical.show_subscription_status('" + target_conf['subscription-name'] + "');")
            result = cur_target.fetchone()
            if result is None:
                raise Exception("There is no replication ongoing within this microservice and target host")
            else:
                res = result[0].split(",")
                cur_target.close()
                return res[4]
    except Exception as e:
        pprint("Error getting replication slot name:" + str(e))
        if "current database is not configured as" in str(e):
            return target_conf['replication-slot']
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def dropSubscription():
    pprint("dropSubscription")
    try:
        cur_target = conn_target.cursor()
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_target.execute(
                "DROP SUBSCRIPTION \"" + target_conf['subscription-name'] + "\";")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute(
                "SELECT pglogical.drop_subscription('" + target_conf['subscription-name'] + "');")
            cur_target.execute(
                "SELECT pglogical.drop_node('" + target_conf['subscription-name'] + "');")
        cur_target.close()
    except Exception as e:
        pprint("Error in cursor dropping subscription:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def kill_fivetran_queries_in_target():
    try:
        cur_target = conn_target.cursor()
        cur_target.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                           "WHERE usename = '{}' and datname = '{}';"
                           .format("fivetran_appl", str(target_conf['database'])))
    except Exception as e:
        pprint("Error killing fivetran queries:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        sys.exit(1)


def alterTableAndSequenceOwnership():
    pprint("alterTableAndSequenceOwnership")

    try:
        cur_target = conn_target.cursor()
        kill_fivetran_queries_in_target()
        for schema in source_schemas:
            cur_target.execute("ALTER SCHEMA \"" + schema + "\" OWNER TO \"" + target_conf['role'] + "\";")
            cur_target.execute(
                "GRANT ALL ON SCHEMA \"" + schema + "\" TO \"" + rep_user + "\";")
            cur_target.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA \"" + schema + "\" GRANT ALL PRIVILEGES ON TABLES TO \"" + rep_user + "\";")
        cur_target.execute(
            "SELECT 'ALTER TABLE IF EXISTS \"' || schemaname || '\".\"' || relname || '\" OWNER TO \"" + target_conf[
                'role'] +
            "\";' as sentence FROM pg_stat_user_tables WHERE relname not like 'aws%';")
        rows = cur_target.fetchall()
        for row in rows:
            cur_target.execute(str(row[0]))

        if target_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:

            cur_target.execute("SELECT 'ALTER SEQUENCE IF EXISTS \"' || schemaname || '\".\"' || sequencename || '\" "
                               "OWNER TO \"" + target_conf['role'] +
                               "\";' as sentence FROM pg_sequences WHERE sequencename not like 'aws%';")
        else:
            cur_target.execute("SELECT 'ALTER SEQUENCE IF EXISTS \"' || quote_ident(nspname) || '\".\"' || "
                               "quote_ident(relname) || '\" OWNER TO \"" + target_conf['role'] +
                               "\";' FROM ( SELECT relname,nspname,d.refobjid::regclass::text as tabl, a.attname, refobjid "
                               "FROM pg_depend d JOIN   pg_attribute a ON a.attrelid = d.refobjid "
                               "AND a.attnum = d.refobjsubid JOIN pg_class r on r.oid = objid "
                               "JOIN pg_namespace n on n.oid = relnamespace "
                               "WHERE d.refobjsubid > 0 and  relkind = 'S') as subq;")
        rows = cur_target.fetchall()
        for row in rows:
            cur_target.execute(str(row[0]))
        cur_target.close()
    except Exception as e:
        pprint("Error altering table and sequences ownership:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def repairOrphans():
    pprint("repairOrphans")

    try:
        cur_target = conn_target.cursor()
        cur_source = conn_source.cursor()
        cur_source.execute("WITH seqs AS (SELECT n.nspname, relname as seqname FROM pg_class c JOIN pg_namespace n on "
                           "n.oid = c.relnamespace WHERE relkind = 'S'), attached_seqs AS ( SELECT n.nspname, "
                           "c.relname as tablename, (regexp_matches(pg_get_expr(d.adbin, d.adrelid), "
                           "'''([^'']+)'''))[1] as seqname FROM pg_class c JOIN pg_namespace n on n.oid = "
                           "c.relnamespace JOIN pg_attribute a on a.attrelid = c.oid JOIN pg_attrdef d on d.adrelid = "
                           "a.attrelid and d.adnum = a.attnum and a.atthasdef WHERE relkind = 'r' and a.attnum > 0 "
                           "and pg_get_expr(d.adbin, d.adrelid) ~ '^nextval') "
                           "SELECT 'SELECT last_value + 1 as last_value, ''' || nspname || '.' || seqname || ''' as seqname FROM "
                           "\"' || nspname || '\".\"' || seqname || '\";' FROM seqs s LEFT JOIN attached_seqs a "
                           "USING(nspname, seqname) WHERE a.tablename IS NULL;")
        rows = cur_source.fetchall()
        kill_fivetran_queries_in_target()
        for row in rows:
            cur_source.execute(str(row[0]))
            rows2 = cur_source.fetchall()
            for row2 in rows2:
                cur_target.execute("SELECT setval('" + str(row2[1]) + "', " + str(row2[0]) + ");")

        cur_target.close()
        cur_source.close()
    except Exception as e:
        pprint("Error repairing orphan sequences:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def alterDatabaseOwnership():
    pprint("alterDatabaseOwnership")

    try:
        cur_target = conn_target.cursor()
        kill_fivetran_queries_in_target()
        cur_target.execute(
            "ALTER DATABASE \"" + target_conf['database'] + "\"  OWNER TO \"" + target_conf['role'] + "\";")
        cur_target.close()
    except Exception as e:
        pprint("Error altering database owner:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def roolbackToBeginning():
    pprint("rollbackToBeginning")

    try:
        conn_postgres_target = openTargetPostgresConn()
        conn_postgres_target.autocommit = True
        cur_postgres_target = conn_postgres_target.cursor()
        cur_postgres_target.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + target_conf['database'] + "';")
        cur_postgres_target.execute(
            "DROP DATABASE \"" + target_conf['database'] + "\";")
        cur_postgres_target.execute("GRANT \"" + target_conf['role'] + "\" TO dba_test_service;")
        cur_postgres_target.execute(
            "CREATE DATABASE \"" + target_conf['database'] + "\" WITH OWNER \"" + target_conf['role'] + "\";")
        cur_postgres_target.close()
        conn_postgres_target.close()
    except Exception as e:
        pprint("Error in rollback to beginning:" + str(e))
        if "cur_postgres_target" in locals():
            cur_postgres_target.close()
        conn_postgres_target.close()
        closeConnections()
        sys.exit(1)


def dropTargetDatabase():
    pprint("dropTargetDatabase")

    try:
        conn_postgres_target = openTargetPostgresConn()
        conn_postgres_target.autocommit = True
        cur_postgres_target = conn_postgres_target.cursor()
        cur_postgres_target.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + target_conf['database'] + "';")
        cur_postgres_target.execute(
            "DROP DATABASE \"" + target_conf['database'] + "\";")
        cur_postgres_target.close()
        conn_postgres_target.close()
    except Exception as e:
        pprint("Error in dropping target database:" + str(e))
        if "cur_postgres_target" in locals():
            cur_postgres_target.close()
        conn_postgres_target.close()
        closeConnections()
        sys.exit(1)


def repairSequences():
    pprint("repairSequences")

    try:
        new_conn_target = openTargetConn(True)
        cur_target = new_conn_target.cursor()
        cur_target.execute("SELECT 'SELECT SETVAL(' || quote_literal(quote_ident(nspname) || '.' || "
                           "quote_ident(relname)) || ', COALESCE(MAX(' ||quote_ident(attname)|| '), 1)) "
                           "FROM ' || tabl || ';' as exec_query "
                           "FROM ( SELECT relname,nspname,d.refobjid::regclass::text as tabl, a.attname, refobjid "
                           "FROM pg_depend d JOIN   pg_attribute a ON a.attrelid = d.refobjid "
                           "AND a.attnum = d.refobjsubid JOIN pg_class r on r.oid = objid "
                           "JOIN pg_namespace n on n.oid = relnamespace "
                           "WHERE  d.refobjsubid > 0 and  relkind = 'S') as subq;")
        rows = cur_target.fetchall()
        kill_fivetran_queries_in_target()
        for row in rows:
            cur_target.execute(str(row[0]))
        cur_target.close()
        new_conn_target.close()
    except Exception as e:
        if "statement timeout" in str(e):
            pprint("Query really slow, killing it and continuing migration.")
        else:
            pprint("Error fixing sequences:" + str(e))
            if "cur_target" in locals():
                cur_target.close()
            if "new_conn_target" in locals():
                new_conn_target.close()
            closeConnections()
            sys.exit(1)


def recreateExtraReplicationSlots():
    pprint("recreateExtraReplicationSlots")

    try:

        slot_name = getReplicationSlotName()
        cur_source = conn_source.cursor()
        cur_source.execute("SELECT 'SELECT pg_create_logical_replication_slot('||quote_literal(quote_ident(slot_name))"
                           "||','||quote_literal(quote_ident(plugin))||');' FROM pg_replication_slots "
                           "WHERE database = '" + str(source_conf['database']) + "' AND slot_name <> '" + str(slot_name)
                           + "' AND slot_type = 'logical';")
        rows = cur_source.fetchall()
        cur_target = conn_target.cursor()
        for row in rows:
            cur_target.execute(str(row[0]))
        cur_source.close()
        cur_target.close()
    except Exception as e:
        pprint("Error recreating extra slots:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        # sys.exit(1)


def dropExtraReplicationSlots():
    pprint("dropExtraReplicationSlots")

    try:

        slot_name = getReplicationSlotName()
        cur_target = conn_target.cursor()
        cur_target.execute("SELECT 'SELECT pg_drop_replication_slot('||quote_literal(quote_ident(slot_name))||');' "
                           "FROM pg_replication_slots WHERE database = '" + str(target_conf['database'])
                           + "' AND slot_name <> '" + str(slot_name) + "' AND slot_type = 'logical';")
        rows = cur_target.fetchall()
        for row in rows:
            cur_target.execute(str(row[0]))
        cur_target.close()
    except Exception as e:
        pprint("Error droppping extra slots:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def terminateSourceRoleConnections():
    pprint("terminateSourceRoleConnections")

    if str(owner_role) != "postgres":
        try:
            cur = conn_source.cursor()
            cur.execute("ALTER ROLE \"" + source_conf['role'] + "\" WITH CONNECTION LIMIT 0;")
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + source_conf['role'] + "';")

            ### Agrego corte para roles no owners
            ms_dict = getDatabaseRegisteredVarsDynamo()
            for each_ms in ms_dict["data"]:
                if each_ms:
                    cur.execute(
                        "SELECT 1 FROM pg_roles WHERE rolname = '" + each_ms['role'] + "';")
                    if cur.fetchone():
                        pprint("Cortando conexiones para el role: {}".format(each_ms['role']))
                        cur.execute("ALTER ROLE \"" + each_ms['role'] + "\" WITH CONNECTION LIMIT 0;")
                        cur.execute(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" +
                            each_ms['role'] + "';")
            cur.close()
        except Exception as e:
            pprint("Error terminating source role connections:" + str(e))
            if "cur" in locals():
                cur.close()
            closeConnections()
            sys.exit(1)


def createExtraRolesOnTarget(ms_dict):
    pprint("createExtraRolesOnTarget")
    try:
        cur_target = conn_target.cursor()
        if ms_dict:
            for each_ms in ms_dict["data"]:
                each_ms = get_role_source_configuration(each_ms)
                if each_ms:
                    cur_target.execute(
                        "SELECT 1 FROM pg_roles WHERE rolname = '" + each_ms['role'] + "';")
                    if cur_target.fetchone() is None:
                        print()
                        print("Creating extra role in target... " + each_ms['role'])
                        print()
                        cur_target.execute("CREATE ROLE \"{}\" LOGIN PASSWORD '{}' INHERIT;".format(each_ms['role'],
                                                                                                    each_ms['password']))
                        cur_target.execute("ALTER ROLE \"{}\" CONNECTION LIMIT {};".format(each_ms['role'],
                                                                                           each_ms['connection_limit']))
                        for each_conf in each_ms['timeouts']:
                            fields = each_conf.split("=")
                            cur_target.execute("ALTER ROLE \"{}\" SET {}='{}';".format(each_ms['role'], fields[0],
                                                                                   fields[1]))
                        cur_target.execute("GRANT writeallaccess TO \"{}\";".format(each_ms['role']))
                        cur_target.execute("GRANT \"{}\" TO postgres;".format(each_ms['role']))
                    else:
                        print("Role \"{}\" already created in target... ".format(each_ms['role']))
        cur_target.close()
    except Exception as e:
        pprint("Error in creating extra roles in target RDS:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        if "conn_source" in globals():
            conn_source.close()
        if "conn_target" in globals():
            conn_target.close()
        sys.exit(1)


def createNominalRolesOnTarget():
    pprint("createNominalRolesOnTarget")
    print()

    try:
        cur_source = conn_source.cursor()
        cur_target = conn_target.cursor()
        cur_source.execute("SELECT rolname FROM pg_roles WHERE rolcanlogin AND rolname NOT SIMILAR TO "
                           "'dba%|postgres|writeallaccess|readaccess|fullreadaccess|toolreadaccess|%appl|%yerson%|"
                           "felipev|felipevillamarin|redash%|fivetran%|retool%|pgpool%|admin|administrator|x1rformance"
                           "|pgwatch2|dbmonitoring|devopsadmin|listman|newrelic_agent' ORDER BY rolname;")
        rows = cur_source.fetchall()
        for row in rows:
            cur_target.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = '" + str(row[0]) + "';")
            if cur_target.fetchone() is None:
                print("Creating nominal role " + str(row[0]) + " in target...")
                cur_target.execute(
                    "CREATE ROLE \"" + str(row[0]) + "\" LOGIN PASSWORD 'ChanGeM3' INHERIT;")
                cur_target.execute("ALTER ROLE \"" + str(row[0]) + "\" CONNECTION LIMIT 100;")
                cur_target.execute("ALTER ROLE \"" + str(row[0]) + "\" SET statement_timeout=60000;")
                cur_target.execute("ALTER ROLE \"" + str(row[0]) + "\" SET lock_timeout=30000;")
                cur_target.execute(
                    "ALTER ROLE \"" + str(row[0]) + "\" SET idle_in_transaction_session_timeout=1200000;")
                cur_target.execute("ALTER ROLE \"" + str(row[0]) + "\" SET work_mem='64MB';")
                cur_target.execute("ALTER ROLE \"" + str(row[0]) + "\" SET random_page_cost='1.1';")
                cur_target.execute("GRANT fullreadaccess TO \"" + str(row[0]) + "\";")
                cur_target.execute("GRANT \"" + str(row[0]) + "\" TO postgres;")

        cur_target.close()
        cur_source.close()
    except Exception as e:
        pprint("Error in creating nominal roles in target RDS:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        if "conn_source" in globals():
            conn_source.close()
        if "conn_target" in globals():
            conn_target.close()
        sys.exit(1)


def fixSequences():
    pprint("fixSequences")


def closeConnections():
    conn_source.close()
    conn_target.close()


def main():
    startMigration()
    closeConnections()


if __name__ == '__main__':
    main()

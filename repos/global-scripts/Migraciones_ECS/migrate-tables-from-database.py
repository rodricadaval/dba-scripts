#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform auto-scaling to docdb replicas on nightshifts.
# Collect vital stats when needed

import os
from ast import literal_eval

import requests
import json
from base64 import b64encode
import boto3
import datetime
import subprocess
import psycopg2
import re
from pprint import pprint
import hashlib
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

os.system('clear')

parser = argparse.ArgumentParser(description='Migrate PostgreSQL Database Microservices to New RDS')

parser.add_argument('-c', '--country', action="store", dest="country",
                    help="Country -> [ar,br,cl,co,cr,ec,xx1,x1,xx1,dev,uy]", required=True)
parser.add_argument('-s', '--source', action="store", dest="source", help="AWS Instance Endpoint", required=True)
parser.add_argument('-t', '--target', action="store", dest="target", help="AWS Instance Endpoint", required=True)
parser.add_argument('-u', '--replication-user', action="store", dest="rep_user", default="dba_test_service",
                    help="Role that is going to perform replication")
parser.add_argument('--source-database', action="store", dest="source_database", help="Source Database Name",
                    required=True)
parser.add_argument('--target-database', action="store", dest="target_database", help="Target Database Name",
                    required=True)
parser.add_argument('--tables', action="store", dest="tables", help="List schema.table separated by ,",
                    required=True)
parser.add_argument('--microservice', action="store", dest="microservice", help="",
                    required=True)
parser.add_argument('-a', '--action', action="store", dest="action",
                    help="Action -> (start, migrate, abort-all, read-ms-vars)",
                    required=True)
parser.add_argument('--no-initial-data', action="store_true", dest="no_initial_data", help="Do not sync existent data",
                    required=False)
parser.add_argument('--no-create-publisher', action="store_true", dest="no_create_publisher", help="Publisher node already exists",
                    required=False)
parser.add_argument('-r', '--role', action="store", dest="source_role", help="Forced Role Name",
                    required=False)
parser.add_argument('-p', '--pass', action="store", dest="source_pass", help="Forced Role Pass",
                    required=False)

args = parser.parse_args()

country = args.country
source = args.source
target = args.target
database = args.source_database
target_database = args.target_database
microservice = args.microservice
tables = args.tables
rep_user = args.rep_user
action = args.action
if args.no_initial_data:
    no_initial_data = True
else:
    no_initial_data = False
if args.no_create_publisher:
    no_create_publisher = True
else:
    no_create_publisher = False
if args.source_role:
    forced_source_role = args.source_role
if args.source_pass:
    forced_source_pass = args.source_pass

git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
if bool(glob(git_root + "/global-scripts/Migraciones_ECS/output*")):
    for f in glob(git_root + "/global-scripts/Migraciones_ECS/output*"):
        os.remove(f)
if bool(glob(git_root + "/global-scripts/Migraciones_ECS/errors*")):
    for f in glob(git_root + "/global-scripts/Migraciones_ECS/errors*"):
        os.remove(f)
out_file = "output.log." + str(time.time())
errors_file = "errors.log." + str(time.time())

try:
    source_endpoint = os.popen("nslookup " + source + " | grep Name | cut -d: -f2 | xargs").readline().strip()
    target_endpoint = os.popen("nslookup " + target + " | grep Name | cut -d: -f2 | xargs").readline().strip()

    if str(source_endpoint) == str(target_endpoint):
        pprint("Source and target cannot be the same RDS Endpoint")
        sys.exit(1)
except Exception as e:
    pprint("Error checking source and target endpoint difference" + str(e))
    sys.exit(1)


def extractMsCredentials(ms_vars2):
    """
    Genera las variables del conector source
    """
    #pprint("extractMsCredentials")
    try:
        conf = dict()
        for key in ['var_database', 'var_role', 'var_host', 'var_port', 'var_password']:
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
    if "anansiblevar_database_password_var_url" in vars.keys():
        conf['var_password_url'] = vars['anansiblevar_database_password_var_url']
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
        return conf_list
    except Exception as e:
        pprint("Error getting microservice vars from requirements:" + str(e))
        sys.exit(1)


def getUserPass():
    """
    Devuelve la password del usuario de replicacion
    """
    #pprint("getUserPass")
    try:
        generateShelveAdminUsers()

        d = shelve.open("/tmp/svpass", writeback=True)
        secret = d[rep_user]
        d.close()

        return secret

    except Exception as e:
        pprint("Error opening shelve file:" + str(e))
        sys.exit(1)

def getDatabaseRegisteredVars():
    """
    Devuelve la password del usuario de replicacion
    """
    #pprint("getDatabaseRegisteredVars")
    try:
        variables = dict()
        file_path = "/tmp/shelvevariablesbytables" + "_" + country
        if os.path.isfile(file_path + ".db") or os.path.isfile(file_path):
            os.popen('sudo chmod 774 /tmp/shelvevariablesbytables' + '_' + '*').readline().strip()
        d = shelve.open("/tmp/shelvevariablesbytables" + "_" + country, writeback=True)
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


def generateShelveAdminUsers():
    """
    Genera un shelve file con los admin users ubicados en pgpass.
    """
    #pprint("generateShelveAdminUsers")

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


def createOrUpdateTargetMSRole():
    #pprint("createOrUpdateTargetMSRole")

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

        cur_postgres_target.execute("ALTER ROLE \"" + target_conf['role'] + "\" CONNECTION LIMIT 100;")
        cur_postgres_target.execute("ALTER ROLE \"" + target_conf['role'] + "\" SET statement_timeout=60000;")
        cur_postgres_target.execute("ALTER ROLE \"" + target_conf['role'] + "\" SET lock_timeout=30000;")
        cur_postgres_target.execute(
            "ALTER ROLE \"" + target_conf['role'] + "\" SET idle_in_transaction_session_timeout=1200000;")
        cur_postgres_target.execute("ALTER ROLE \"" + target_conf['role'] + "\" SET work_mem='64MB';")
        cur_postgres_target.execute("ALTER ROLE \"" + target_conf['role'] + "\" SET random_page_cost='1.1';")
        cur_postgres_target.execute("GRANT writeallaccess TO \"" + target_conf['role'] + "\";")
        cur_postgres_target.execute("GRANT \"" + target_conf['role'] + "\" TO postgres;")

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


rep_user_pass = getUserPass()


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


def openTargetConn():
    """
    Apertura de target connector
    """
    #pprint("openTargetConn")
    try:
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

def generateSourceConfig():
    """
    Genera las variables del conector source y los microservicios que acceden
    """
    #pprint("generateSourceConfig")
    try:
        msvars = dict()
        msvars = getDatabaseRegisteredVars()
        global source_conf

        if "forced_source_role" in globals() and "forced_source_pass" in globals():
            source_conf['database'] = database
            source_conf['role'] = forced_source_role
            source_conf['publication-name'] = database + '_publication_' + str(microservice).replace("-", "_")
            source_conf['host'] = source
            source_conf['port'] = 5432
            source_conf['password'] = forced_source_pass
            global owner_role
            owner_role = forced_source_role
        elif str(owner_role) == "postgres":
            source_conf['database'] = database
            source_conf['role'] = owner_role
            source_conf['publication-name'] = database + '_publication_' + str(microservice).replace("-", "_")
            source_conf['host'] = source
            source_conf['port'] = 5432

        d = shelve.open("/tmp/shelvevariablesbytables" + "_" + country, writeback=True)
        if not msvars:
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
                                       '| awk -F/ \'{print $NF}\'').readline().strip()
                if temp_result is not None and temp_result:
                    if str(temp_result) == str(microservice):
                        ms_w_var = dict()
                        ms_w_var['ms'] = temp_result
                        ms_w_var['database_var'] = each_var
                        ms_names.append(ms_w_var)

            for each_ms in ms_names:
                ms_variables_list = pullMicroserviceCredentials(each_ms)
                for ms_variables in ms_variables_list:
                    temp = extractMsCredentials(ms_variables)
                    temp_endpoint = os.popen("nslookup " + temp['host'] + " | grep Name | cut -d: -f2 | xargs").readline().strip()
                    if str(temp_endpoint) == str(source_endpoint):
                        source_conf['database'] = temp['database']
                        source_conf['role'] = temp['role']
                        source_conf['publication-name'] = database + '_publication_' + str(microservice).replace("-", "_")
                        source_conf['host'] = temp['host']
                        source_conf['port'] = temp['port']
                        source_conf['password'] = temp['password']
                        msvarslist.append(temp)
            list_sources = []

            if str(database) not in d.keys():
                d[database] = []
                temp = dict()
                temp[source_endpoint] = msvarslist
                temp['last_check'] = time.time()
                d[database].append(temp)
            else:
                expectedResult = list(filter(lambda d: list(d)[0] in [source_endpoint], d[database]))
                if expectedResult:
                    for each_env in expectedResult:
                        if each_env.keys() == str(source_endpoint):
                            each_env[source_endpoint] = msvarslist
                            each_env['last_check'] = time.time()
                        list_sources.append(each_env)
                else:
                    each_env = dict()
                    each_env[source_endpoint] = msvarslist
                    each_env['last_check'] = time.time()
                    list_sources = d[database]
                    list_sources.append(each_env)
                d[database] = list_sources
        else:
            if str(owner_role) != "postgres":
                res = [sub[source_endpoint] for sub in d[database]]
                for each_ms in res[0]:
                    source_conf = each_ms
                    source_conf['publication-name'] = each_ms['database'] + '_publication_' + str(microservice).replace("-", "_")
        d.close()
    except Exception as e:
        pprint("Error generating source config and extra ms:" + str(e))
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
        if no_create_publisher:
            conf['subscription-name'] = str(database).replace("-", "_") + "_subscription_" + str(microservice).replace(
                "-", "_") + "_" + hashlib.md5(str(tables).encode()).hexdigest()
        else:
            conf['subscription-name'] = str(database).replace("-", "_") + "_subscription_" + str(microservice).replace("-", "_")
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


def generate_db_structure():
    list_tables = tables.split(",")
    db_struct = dict()
    for table in list_tables:
        splitted = table.split(".")
        if len(splitted) == 1:
            if "public" not in db_struct.keys():
                db_struct["public"] = [str(splitted[0])]
            else:
                db_struct['public'].append(str(splitted[0]))
        elif len(splitted) == 2:
            if str(splitted[0]) not in db_struct.keys():
                db_struct[splitted[0]] = [str(splitted[1])]
            else:
                db_struct[splitted[0]].append(str(splitted[1]))
    return db_struct


def getSourceSchemas():
    """
    Obtiene los schemas de la db source y los guarda en un listado
    """
    try:
        return generate_db_structure()

    except Exception as e:
        pprint("Error in cursor getting source schemas:" + str(e))
        sys.exit(1)


def checkMissingPks():
    #pprint("checkMissingPks")

    try:
        list_schemas = generate_db_structure()
        in_query = []
        for schema in list_schemas:
            for table in list_schemas[schema]:
                in_query.append(str(schema)+"."+str(table))
        cur = conn_source.cursor()
        if source_conf['version'].split(".")[0] in ["11", "12", "10", "13"]:
            cur.execute("select tab.table_schema || '.' || tab.table_name as table from information_schema.tables tab "
                        "left join information_schema.table_constraints tco on tab.table_schema = tco.table_schema and "
                        "tab.table_name = tco.table_name and tco.constraint_type = 'PRIMARY KEY' where "
                        "tab.table_type = 'BASE TABLE' and tab.table_schema not in ('pg_catalog', 'information_schema') "
                        "and tco.constraint_name is null AND tab.table_schema || '.' || tab.table_name = ANY (%s) "
                        "order by tab.table_schema, tab.table_name;", (in_query,))
        else:
            cur.execute("SELECT n.nspname || '.' || c.relname as table FROM pg_catalog.pg_class c JOIN pg_namespace n "
                        "ON ( c.relnamespace = n.oid AND n.nspname NOT IN "
                        "('information_schema', 'pg_catalog', 'pglogical', 'tiger', 'topology', 'pg_toast') "
                        "AND c.relkind='r' ) WHERE c.relhaspkey = false AND n.nspname || '.' || c.relname = ANY (%s);"
                        , (in_query,))
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


owner_role = ""
generateSourceConfig()
owner_role = source_conf['role']
target_conf = generateTargetConfig()
target_conf['database'] = target_database
target_conf['role'] = source_conf['role']
target_conf['port'] = source_conf['port']
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
        createExtraRolesOnTarget(getDatabaseRegisteredVars())
        dumpRestore()
        createPublication()
        alterTargetDatabaseOwner()
        createSubscriber()
    elif action == "migrate":
        terminateSourceRoleConnections()
        waitTillCatchUp()
        dropSubscription()
        dropPublication()
        alterTableAndSequenceOwnership()
        alterDatabaseOwnership()
        repairSequences()
    elif action == "skip-prmissions":
        if not no_create_publisher:
            createPublication()
        alterTargetDatabaseOwner()
        createSubscriber()
    elif action == "create-publication":
        createPublication()
    elif action == "create-subscription":
        createSubscriber()
    elif action == "drop-publication":
        if not no_create_publisher:
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
    elif action == "abort-all":
        dropSubscription()
        if not no_create_publisher:
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
        showReplicationStatus()
    elif action == "check-all-roles":
        microdict = getDatabaseRegisteredVars()
        if microdict:
            pprint(microdict)
        else:
            print("There are no extra roles to create.")
    elif action == "create-extra-roles":
        createExtraRolesOnTarget(getDatabaseRegisteredVars())
    elif action == "dev-create-nominal-roles":
        createNominalRolesOnTarget()
    elif action == "repair-ms-target-role":
        createOrUpdateTargetMSRole()
    elif action == "move-schema-only":
        adjustPermissions()
        createExtraRolesOnTarget(getDatabaseRegisteredVars())
        dumpRestore()
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
    grantReplication(cur_source, conn_source)
    grantReplication(cur_target, conn_target)
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
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_admin_roles_tools_settings.sql " + value + ":/tmp/pg_fix_admin_roles_tools_settings.sql")
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_grant_roles_new_rds.sql " + value + ":/tmp/pg_fix_grant_roles_new_rds.sql")
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_tools_roles.sql " + value + ":/tmp/pg_fix_tools_roles.sql")
        if target_conf['version'].split(".")[0] in ["11", "12", "10"]:
            logCommand(
                "sudo docker cp " + git_root + "/postgresql/common/pg_fix_global_views_pg12.sql " + value + ":/tmp/pg_fix_global_views.sql")
        elif target_conf['version'].split(".")[0] in ["13"]:
            logCommand(
                "sudo docker cp " + git_root + "/postgresql/common/pg_fix_global_views_pg13.sql " + value + ":/tmp/pg_fix_global_views.sql")
        else:
            logCommand(
                "sudo docker cp " + git_root + "/postgresql/common/pg_fix_global_views.sql " + value + ":/tmp/pg_fix_global_views.sql")
        logCommand(
            "sudo docker cp " + git_root + "/postgresql/common/pg_fix_public_grants.sql " + value + ":/tmp/pg_fix_public_grants.sql")
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
                       'database'] + " -f " + "/tmp/pg_fix_admin_roles_tools_settings.sql\"")
        logCommand("sudo docker exec -it " + getDockerName(
            target_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                       'host'] + " -U " + rep_user + " -d " + target_conf[
                       'database'] + " -f " + "/tmp/pg_fix_grant_roles_new_rds.sql\"")
        logCommand("sudo docker exec -it " + getDockerName(
            target_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                       'host'] + " -U " + rep_user + " -d " + target_conf[
                       'database'] + " -f " + "/tmp/pg_fix_tools_roles.sql\"")
        logCommand("sudo docker exec -it " + getDockerName(
            target_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                       'host'] + " -U " + rep_user + " -d " + target_conf[
                       'database'] + " -f " + "/tmp/pg_fix_global_views.sql\"")
        logCommand("sudo docker exec -it " + getDockerName(
            target_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                       'host'] + " -U " + rep_user + " -d " + target_conf[
                       'database'] + " -f " + "/tmp/pg_fix_public_grants.sql\"")
    except Exception as e:
        pprint("Error executing exported files in target:" + str(e))
        closeConnections()
        sys.exit(1)


def sendFilesOnSource():
    """
    Copia los files desde el repo hacia el docker correspondiente. Segun connector pg version
    """
    pprint("sendFilesOnSource")
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


def dumpRestore():
    """
    Aplica dump en source y restore en target. Se conecta al docker del source
    """
    try:
        pprint("dumpRestore")
        cur_target = conn_target.cursor()
        for schema in source_schemas.keys():
            if str(schema) != "public":
                cur_target.execute("CREATE SCHEMA \"" + schema + "\";")
                cur_target.execute("GRANT ALL ON ALL TABLES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
                cur_target.execute("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
        cur_target.close()

        dump_tables = ""
        for schema in source_schemas:
            for table in source_schemas[schema]:
                dump_tables += " -t " + table

        logCommand(
            "sudo docker exec -it " + getDockerName(
                source_conf['version']) + " sh -c \"PGPASSWORD=" + rep_user_pass + " pg_dump -h " + source_conf[
                'host'] + " -s --no-owner -U " + rep_user + dump_tables +" --role=postgres -d " +
            source_conf[
                'database'] + " | PGPASSWORD=" + rep_user_pass + " psql -h " + target_conf[
                'host'] + " -U " + rep_user + " -d " + target_conf['database'] + "\"")

    except Exception as e:
        if "schema" not in str(e) and "already exists" not in str(e):
            pprint("Error in dump-restore segment:" + str(e))
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
        p.wait(timeout=30000)
        out.close()
        error.close()
    except Exception as e:
        pprint("Error with subprocess popen command:" + str(e))
        closeConnections()
        sys.exit(1)


def grantReplication(cursor, conn):
    """
    Aplica los grant para permitir la replicacion al replication user especificado
    """
    try:
        pprint("grantReplication")

        cursor.execute("GRANT rds_replication TO dba_test_service;")
        cursor.execute("GRANT postgres TO dba_test_service;")
        cursor.execute("GRANT rds_superuser TO dba_test_service;")
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
                                   "pg_catalog.pg_replication_slots WHERE slot_name = '" + target_conf[
                                       'replication-slot'] + "';")
            elif source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
                cur_source.execute("SELECT slot_name, confirmed_flush_lsn as flushed, pg_current_wal_lsn(), "
                                   "(pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance FROM "
                                   "pg_catalog.pg_replication_slots WHERE slot_name = '" + target_conf[
                                       'replication-slot'] + "';")

            delay = cur_source.fetchone()[3]
            if delay > 0:
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
    result = round(decimal.Decimal(before) / (decimal.Decimal(before) - decimal.Decimal(now)))
    print("ETA -> " + str(result) + " seconds")


def createPublication():
    pprint("createPublication")
    try:
        cur_source = conn_source.cursor()
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_source.execute("CREATE PUBLICATION \"" + source_conf['publication-name'] + "\";")
            for schema in source_schemas:
                for table in source_schemas[schema]:
                    cur_source.execute("ALTER PUBLICATION \"" + source_conf['publication-name'] + "\" ADD TABLE " +
                                       str(schema) + "." + str(table) + ";")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_source.execute("CREATE EXTENSION IF NOT EXISTS pglogical;")
            cur_source.execute("SELECT pglogical.create_node(node_name := '" + source_conf['publication-name']
                               + "', dsn := 'host=" + source_conf['host']
                               + " port=" + str(source_conf['port']) + " user=" + rep_user + " password=" + rep_user_pass
                               + " dbname=" + source_conf['database'] + "');")
            for schema in source_schemas:
                for table in source_schemas[schema]:
                    cur_source.execute(
                        "SELECT pglogical.replication_set_add_table('default','" + str(schema) + "." + str(table) + "');")

            if "spatial_ref_sys" in str(tables):
                cur_source.execute("SELECT 1 FROM pg_tables WHERE tablename = 'spatial_ref_sys';");
                if cur_source.fetchone():
                    cur_source.execute("SELECT pglogical.replication_set_remove_table('default', 'public.spatial_ref_sys');")

        cur_source.close()
    except Exception as e:
        pprint("Error in cursor creating publication:" + str(e))
        if "cur_source" in locals():
            cur_source.close()
        closeConnections()
        sys.exit(1)


def createSubscriber():
    pprint("createSubscriber")
    try:
        cur_target = conn_target.cursor()
        for schema in source_schemas:
            cur_target.execute("GRANT ALL ON ALL TABLES IN SCHEMA \"" + schema + "\" TO " + rep_user + ";")
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            if not no_initial_data:
                cur_target.execute(
                    "CREATE SUBSCRIPTION \"" + target_conf['subscription-name'] + "\" CONNECTION 'host=" + source_conf['host'] +
                    " port=5432 dbname=" + source_conf['database'] + " user=" + rep_user + " password=" + rep_user_pass +
                    "' PUBLICATION " + source_conf['publication-name'] + ";")
            else:
                cur_target.execute(
                    "CREATE SUBSCRIPTION \"" + target_conf['subscription-name'] + "\" CONNECTION 'host="
                    + source_conf['host'] + " port=5432 dbname=" + source_conf['database']
                    + " user=" + rep_user + " password=" + rep_user_pass + "' PUBLICATION \""
                    + source_conf['publication-name'] + "\" WITH (copy_data = false);")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute("CREATE EXTENSION IF NOT EXISTS pglogical;")
            cur_target.execute("SELECT pglogical.create_node(node_name := '" + target_conf['subscription-name']
                               + "', dsn := 'host=" + target_conf['host'] + " port=" + str(target_conf['port'])
                               + " user=" + rep_user + " password=" + rep_user_pass + " dbname="
                               + target_conf['database'] + "');")
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


def showReplicationStatus():
    pprint("showReplicationStatus")

    try:
        cur_target = conn_target.cursor()
        if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_target.execute("SELECT * FROM pg_subscription sub INNER JOIN pg_stat_subscription stat ON stat.subid = "
                               "sub.oid WHERE sub.subname = '" + target_conf['subscription-name'] + "';")
        elif source_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute("SELECT *, (SELECT string_agg(sync_nspname || '.' ||sync_relname::text,';') FROM "
                               "pglogical.local_sync_status) FROM pglogical.show_subscription_status('" +
                               target_conf['subscription-name'] + "');")
        result = cur_target.fetchone()
        if result is None:
            print("There is no replication ongoing within this microservice and target host")
        else:
            if source_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
                print()
                print("Subscriber Node: " + str(result[2]))
                print("State: " + str(result[4]))
                print("Publication Node: " + str(result[8]))
                print("Synchronous Commit: " + str(result[7]))
                print("Slot Name: " + result[6])
                print("Received lsn: " + result[13])
                print("last_msg_send_time: " + str(result[14]))
                print("last_msg_receipt_time: " + str(result[15]))
            else:
                res = str(result).split(",")
                print()
                print("Subscriber Node: " + res[0].replace("(", ""))
                print("State: " + res[1])
                print("Publication Node: " + res[2])
                print("Slot Name: " + res[4])
                print("Tables: " + res[9].replace(")", ""))
        cur_target.close()
    except Exception as e:
        pprint("Error checking replication status:" + str(e))
        if "cur_target" in locals():
            cur_target.close()
        closeConnections()
        sys.exit(1)


def getReplicationSlotName():
    #pprint("getReplicationSlotName")

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


def alterTableAndSequenceOwnership():
    pprint("alterTableAndSequenceOwnership")

    try:
        cur_target = conn_target.cursor()
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
        elif target_conf['version'].split(".")[0] in ["9"]:
            cur_target.execute("SELECT 'ALTER SEQUENCE IF EXISTS \"' || sequence_schema || '\".\"' || sequence_name || '\" "
                               "OWNER TO \"" + target_conf['role'] +
                               "\";' as sentence FROM information_schema.sequences WHERE sequence_name not like 'aws%';")
        rows = cur_target.fetchall()
        for row in rows:
            cur_target.execute(str(row[0]))

        if target_conf['version'].split(".")[0] in ["10", "11", "12", "13"]:
            cur_target.execute("SELECT 'ALTER SEQUENCE IF EXISTS \"' || schemaname || '\".\"' || sequencename || '\" "
                               "AS bigint;' as sentence FROM pg_sequences WHERE sequencename not like 'aws%';")
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


def alterDatabaseOwnership():
    pprint("alterDatabaseOwnership")

    try:
        cur_target = conn_target.cursor()
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
        cur_target = conn_target.cursor()
        cur_target.execute("SELECT 'SELECT SETVAL(' || quote_literal(quote_ident(nspname) || '.' || "
                           "quote_ident(relname)) || ', COALESCE(MAX(' ||quote_ident(attname)|| '), 1)) "
                           "FROM ' || quote_ident(tabl) || ';' as exec_query "
                           "FROM ( SELECT relname,nspname,d.refobjid::regclass::text as tabl, a.attname, refobjid "
                           "FROM pg_depend d JOIN   pg_attribute a ON a.attrelid = d.refobjid "
                           "AND a.attnum = d.refobjsubid JOIN pg_class r on r.oid = objid "
                           "JOIN pg_namespace n on n.oid = relnamespace "
                           "WHERE  d.refobjsubid > 0 and  relkind = 'S') as subq;")
        rows = cur_target.fetchall()
        for row in rows:
            cur_target.execute(str(row[0]))

        cur_target.close()
    except Exception as e:
        pprint("Error fixing sequences:" + str(e))
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
        for each_ms in ms_dict[source_endpoint]:
            cur_target.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = '" + each_ms['role'] + "';")
            if cur_target.fetchone() is None:
                print()
                print("Creating extra role in target... " + each_ms['role'])
                print()
                cur_target.execute(
                    "CREATE ROLE \"" + each_ms['role'] + "\" LOGIN PASSWORD '" + each_ms[
                        'password'] + "' INHERIT;")
                cur_target.execute("ALTER ROLE \"" + each_ms['role'] + "\" CONNECTION LIMIT 100;")
                cur_target.execute("ALTER ROLE \"" + each_ms['role'] + "\" SET statement_timeout=60000;")
                cur_target.execute("ALTER ROLE \"" + each_ms['role'] + "\" SET lock_timeout=30000;")
                cur_target.execute(
                    "ALTER ROLE \"" + each_ms['role'] + "\" SET idle_in_transaction_session_timeout=1200000;")
                cur_target.execute("ALTER ROLE \"" + each_ms['role'] + "\" SET work_mem='64MB';")
                cur_target.execute("ALTER ROLE \"" + each_ms['role'] + "\" SET random_page_cost='1.1';")
                cur_target.execute("GRANT writeallaccess TO \"" + each_ms['role'] + "\";")
                cur_target.execute("GRANT \"" + each_ms['role'] + "\" TO postgres;")
            else:
                print("Role " + each_ms['role'] + " already created in target... ")

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

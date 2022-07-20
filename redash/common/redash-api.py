#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Redash-ci usage
import argparse
import json
import logging
from pprint import pprint
from redashAPI import RedashAPIClient
import Engine
import sys
import os
import yaml
import dpath.util
import subprocess
import traceback
import psycopg2

git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import dynamo
from lib.rds import RDSClient
from lib.requirements.engine import EngineExporter
from pygments import highlight, lexers, formatters

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.basicConfig(
    format='%(asctime)-2s %(name)-2s %(levelname)-4s: %(message)s',
    level=logging.INFO, #Nivel de los eventos que se registran en el logger
)

# Create API client instance
"""
    :args:
    API_KEY
    REDASH_HOST (optional): `http://localhost:5000` by default
"""

API_KEY = ""
REDASH_HOST = ""

def get_country():
    if args.country is None:
        return 'c'
    else:
        return p_country

def get_api_key():
    global API_KEY
    i_dynamo = dynamo.DynamoClient(get_country())
    data = i_dynamo.get_vars('redash_test_user_api_key')
    if not data:
        raise Exception("No se encuentra la key")
    API_KEY = json.loads(data)['value']

def get_api_host():
    global REDASH_HOST
    i_dynamo = dynamo.DynamoClient(get_country())
    data = i_dynamo.get_vars('redash_test_user_host')
    if not data:
        raise Exception("No se encuentra el redash host")
    REDASH_HOST = json.loads(data)['value']


parser = argparse.ArgumentParser(description='Redash CLI')

parser.add_argument('-k', '--object_id', action="store", dest="datid", help="Integer", required=False)
parser.add_argument('-t', '--type', action="store", dest="type", default='all', help="Type -> pg, mysql, mongodb, "
                                                                      "sqlserver, prometheus, redshift, "
                                                                      "mssql, google_spreadsheets, "
                                                                      "mssql_odbc, rds_mysql, "
                                                                      "elasticsearch, influxdb",
                    required=False)
parser.add_argument('-e', '--host', action="store", dest="host", help="AWS Instance Endpoint",
                    required=False)
parser.add_argument('-d', '--database', action="store", dest="database", help="AWS Instance Endpoint",
                    required=False)
parser.add_argument('-u', '--user', action="store", dest="user", help="AWS Instance Endpoint",
                    required=False)
parser.add_argument('-s', '--secret', action="store", dest="secret", help="AWS Instance Endpoint",
                    required=False)
parser.add_argument('-p', '--port', action="store", dest="port", help="Action -> (get, create)",
                    required=False)
parser.add_argument('-a', '--action', action="store", dest="action", help="Action -> (get, create)",
                    required=False)
parser.add_argument('-f', '--free-expression', action="store", dest="free_expression", help="Query", required=False)
parser.add_argument('-n', '--name', action="store", dest="name", help="Name of datasource", required=False)
parser.add_argument('-m', '--microservice', action="store", dest="microservice", help="Name of microservice",
                    required=False)
parser.add_argument('-c', '--country', action="store", dest="country", help="Countries", required=False)
parser.add_argument('--skip-confirmation', action="store_true", dest="skip", help="Skip confirmation", required=False)

args = parser.parse_args()

if args.datid is not None:
    p_datid = args.datid
if args.type is not None:
    p_type = args.type
    if p_type == "docdb":
        p_type = "mongodb"
if args.host is not None:
    p_host = args.host
if args.database is not None:
    p_database = args.database
if args.user is not None:
    p_user = args.user
if args.secret is not None:
    p_varsecret = args.secret
if args.port is not None:
    p_port = args.port
if args.action is not None:
    p_action = args.action
else:
    raise argparse.ArgumentError(args.action, "Parameter action needs to be defined")
if args.name is not None:
    p_name = args.name
if args.free_expression is not None:
    p_free_expression = args.expression
if args.microservice is not None:
    microservice = args.microservice
if args.country is not None:
    p_country = args.country

get_api_key()
get_api_host()

Redash = RedashAPIClient(API_KEY, REDASH_HOST)

def get_rds_credentials():
    global rds_data
    try:
        i_dynamo = dynamo.DynamoClient(p_country)
        rds_data['role'] = json.loads(i_dynamo.get_vars('pg_redash_role'))['value']
        rds_data['secret'] = json.loads(i_dynamo.get_vars('pg_redash_password'))['value']
        rds_data['host'] = json.loads(i_dynamo.get_vars('pg_redash_host'))['value']
        rds_data['database'] = json.loads(i_dynamo.get_vars('pg_redash_database'))['value']

    except Exception as e:
        logging.info("No se encontraron variables del user para el pais seleccionado. Detalle: {}".format(e))
        logging.info(traceback.format_exc())
        sys.exit(1)

rds_data = dict()
get_rds_credentials()

RDS_HOST = rds_data['host']
RDS_USER = rds_data['role']
RDS_SECRET = rds_data['secret']
RDS_DATABASE = rds_data['database']

def search_if_already_exists(ds_info, host, database, engine):
    res = Redash.get('data_sources').json()
    for each in res:
        if ds_info['type'] == each['type']:
            if ds_info['name'].lower().split(" ")[1] in each['name'].lower():
                res = Redash.get('data_sources/{}'.format(each['id'])).json()
                if engine.get_type.same_host(host, database, res['options']):
                    return res
                return None

def open_conn():
    conn = psycopg2.connect(
        "dbname='{}' host='{}' user='{}' password='{}'"
            .format(RDS_DATABASE, RDS_HOST, RDS_USER, RDS_SECRET))
    conn.autocommit = True
    return conn


def create_datasource(ddata=None):
    # Create New Data Source
    global p_name
    global p_user
    global p_host
    global p_varsecret
    global p_database
    global p_port
    global p_country

    if args.user is None:
        p_user = 'redash_appl'
        i_dynamo = dynamo.DynamoClient(p_country)
        var = json.loads(i_dynamo.get_vars('redash_test_user_read_password'))
        p_varsecret = var['value']
    if "p_database" not in globals():
        p_database = ""

    if not ddata:
        if args.name is None:
            raise argparse.ArgumentError(args.name, "Name of datasource is Required")
    else:
        area_part = ""
        team_part = ""
        if ddata['area'].lower():
            area_part = "[{}]".format(ddata['area'].lower())
        if ddata['team'].lower():
            team_part = "[{}]".format(ddata['team'].lower())
        area_team = "{}{}".format(area_part, team_part)
        p_name = "[{}]{} {}-{}".format(ddata['type'], area_team, microservice.lower(), p_country.lower())
        p_host = ddata['host']
        p_database = ddata['database']
        p_port = ddata['port']

    try:
        engine = Engine.EngineAbstract(ddata['type'])
        p_host = get_replica_host(p_host, engine._type_description)[ddata['type']]

        if args.port is not None:
            options = engine.get_type.get_options(p_database, p_host, p_user, p_varsecret, p_port)
        else:
            options = engine.get_type.get_options(p_database, p_host, p_user, p_varsecret)
    except Exception as e:
        logging.error("Datasource {} with type: {} has a connectivity problem: {}. Traceback: {}".format(p_name, ddata['type'], e, traceback.format_exc()))
        return None
    finally:
        new_ds_conf = {'name': "{}".format(p_name), 'type': "{}".format(get_type(ddata['type'])), 'options': options}
        exist_data = search_if_already_exists(new_ds_conf, p_host, p_database, engine)
        if not exist_data:
            if not args.skip:
                print("")
                colorful_json = highlight(json.dumps(new_ds_conf, indent=2, default=str), lexers.JsonLexer(),
                                          formatters.TerminalFormatter())
                print(colorful_json)
                while True:
                    print("")
                    qr = input('Do you wanna create it? Enter yes or no: ')
                    if qr == '' or not qr.lower() in ['y', 'n', 'yes', 'no']:
                        print('Please answer with yes or no!')
                    else:
                        break
                if qr.lower() in ['n', 'no']:
                    logging.info("Rejected")
                    return None
            if "qr" in locals() or args.skip:
                res = Redash.post('data_sources', new_ds_conf).json()
                logging.info("New datasource created with id: {} and name: {}".format(res['id'], res['name']))
                return {"data": ddata, "res": res}
        else:
            logging.info("Datasource already exists with this info: id -> {}, name -> '{}'".format(exist_data['id'],
                                                                                                   exist_data['name']))

            return {"data": ddata, "res": exist_data}


def get_datasource():
    # Retrieve specific Data Source
    res = Redash.get('data_sources/{}'.format(p_datid))
    print(res.json())


def delete_datasource():
    # Delete specific Data Source
    res = Redash.delete('data_sources/{}'.format(p_datid))
    pprint(res)
    logging.info("Done")


def get_all_datasources():
    # Retrieve all Data Sources
    res = Redash.get('data_sources')
    print(res.json())


def get_user():
    # Retrieve specific User
    res = Redash.get('users/{}'.format(p_datid))
    print(res.json())


def get_all_users():
    # Retrieve all users
    res = Redash.get('users')
    print(res.json())


def get_query():
    # Retrieve specific Query
    res = Redash.get('queries/{}'.format(p_datid))
    print(res.json())


def delete_query():
    # Delete query
    res = Redash.delete('queries/{}'.format(p_datid))
    pprint(res)


def get_engine(type=None):
    if p_type == "pg" or type == "pg" or type == "postgres":
        return "postgres"
    elif p_type == "mysql" or type == "mysql":
        return "mysql"
    elif p_type == "mongodb" or type == "mongodb":
        return "mongodb"
    elif p_type == "rds_mysql" or type == "rds_mysql":
        return "mysql"
    elif p_type == "sqlserver" or type == "sqlserver":
        return "sqlserver"
    elif type == "docdb":
        return "docdb"
    elif p_type == "all":
        return "all"
    else:
        return None

def get_type(data):
    if data == "postgres":
        return "pg"
    elif data == "mysql":
        return "mysql"
    elif data == "mongodb":
        return "mongodb"
    elif data == "rds_mysql":
        return "mysql"
    elif data == "sqlserver":
        return "sqlserver"
    else:
        return None


def get_all_queries():
    # Retrieve all Queries
    res = Redash.get('queries')
    print(res.json())


def create_query():
    # Create New Query
    global p_user
    global p_host
    global p_varsecret
    global p_database
    global p_port

    if args.name is None:
        raise argparse.ArgumentError(args.name, "Name of query is Required")
    if args.free_expression is None:
        raise argparse.ArgumentError(args.free_expression, "Parameter -f (--free-expression) -> Query text is Required")
    res = Redash.create_query(p_datid, p_name, p_free_expression)
    pprint(res.json())


def refresh_query():
    # Refresh query
    res = Redash.refresh_query(p_datid)
    print(res.json())


def generate_var_values(vars):
    """
    Armo las vars
    """
    conf = dict()
    conf['var_host'] = vars['anansiblevar_database_host_var']
    conf['var_database'] = vars['anansiblevar_database_name_var']
    if "anansiblevar_database_password_var" in vars.keys():
        conf['var_password'] = vars['anansiblevar_database_password_var']
        if "_url" == vars['anansiblevar_database_password_var'][-4:]:
            conf['var_password'] = vars['anansiblevar_database_password_var'].replace("_url", "")
    if "anansiblevar_database_password_var_url" in vars.keys():
        conf['var_password_url'] = vars['anansiblevar_database_password_var_url']
    conf['var_port'] = vars['anansiblevar_database_port_var']
    conf['var_role'] = vars['anansiblevar_database_user_var']
    return conf


def get_replica_host(host, data_type=None):
    try:
        dict_result = {}
        rdscli = RDSClient(p_country.lower())
        address = os.popen("nslookup " + host + " | grep Name | cut -d: -f2 | xargs").readline().strip()
        if "rds.amazonaws.com" in address:
            instance = address.split('.')[0]
            list_engines = [get_engine(data_type)]
            for engine in list_engines:
                result = rdscli.find_in_dynamo(instance, engine)
                if result:
                    if "ReadReplicaDBInstanceIdentifiers" in result.keys():
                        if result['ReadReplicaDBInstanceIdentifiers']:
                            result2 = rdscli.find_in_dynamo(result['ReadReplicaDBInstanceIdentifiers'][0], get_engine(data_type))
                            if result2:
                                dict_result[engine] = result2['Endpoint']['Address']
                            else:
                                dict_result[engine] = result['Endpoint']['Address']
                        else:
                            dict_result[engine] = result['Endpoint']['Address']
                    else:
                        dict_result[engine] = result['Endpoint']['Address']
            return dict_result
        else:
            dict_result[get_engine(data_type)] = host
            return dict_result
    except Exception as e:
        print(traceback.format_exc())

def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True


def pull_microservice_credentials(is_microservice=True):
    global microservice
    """
    Genera el objeto del microservicio
    """
    try:
        if is_microservice:
            cmd = "python3 {}/global-scripts/get_microservice_data.py --microservice {} --country {} --omit-pass " \
                "--engine {}".format(git_root, microservice, p_country.lower(), get_engine())
        else:
            cmd = "python3 {}/global-scripts/get_microservice_data.py --database {} --country {} --omit-pass " \
                  "--engine {}".format(git_root, p_database, p_country.lower(), get_engine())
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, encoding='utf-8')
        out, err = p.communicate()
        if is_json(out):
            result = json.loads(out)
            ms_data = result[microservice]
            parsed_ms = dict()
            l = ['area', 'team', 'tier', 'vertical']
            engines = set(ms_data) - set(l)
            for item in engines:
                if 'vertical' in ms_data.keys():
                    parsed_ms['area'] = ms_data['vertical']
                    del ms_data['vertical']
                if 'area' in ms_data.keys():
                    parsed_ms['area'] = ms_data['area']
                if 'team' in ms_data.keys():
                    parsed_ms['team'] = ms_data['team']
                if "monolith" in ms_data[item].keys():
                    address = os.popen("nslookup " + ms_data[item]['monolith']['host'] + " | grep Name | cut -d: -f2 | xargs").readline().strip()
                    if "order" not in address and "order" not in address and "core" not in address and "grability" \
                            not in address and "X1Xl4" not in address and "brazil" not in address:
                        if "original" not in ms_data[item].keys():
                            ms_data[item]['original'] = ms_data[item]['monolith']
                if "original" in ms_data[item].keys():
                    if item not in parsed_ms.keys():
                        parsed_ms[item] = dict()
                    parsed_ms[item]['host'] = ms_data[item]['original']['host']
                    parsed_ms[item]['port'] = ms_data[item]['original']['port']
                    parsed_ms[item]['database'] = ms_data[item]['original']['database']
            return parsed_ms
        else:
            logging.error("{}, microservice: {}, engine: {}".format(out, microservice, get_engine()).replace("\n",""))
    except Exception as e:
        logging.error("{}".format(e))
        logging.error(traceback.format_exc())
        sys.exit(1)


def check_dict_path(d, *indices):
    sentinel = object()
    for index in indices:
        d = d.get(index, sentinel)
        if d is sentinel:
            return False
    return True


def execute_query_and_wait():
    # Execute query and wait result
    if args.free_expression is None:
        raise argparse.ArgumentError(args.free_expression, "Parameter -f (--free-expression) -> Query text is Required")
    res = Redash.query_and_wait_result(p_datid, p_free_expression, 120)
    print(res.json())


def get_group(group_id=None):
    conn = open_conn()
    with conn.cursor() as cur:
        if group_id:
            cur.execute("SELECT * FROM groups WHERE id = {};".format(int(group_id)))
            res = cur.fetchall()
            logging.info(res)


def get_groups_by_datasource(ds_id=None):
    conn = open_conn()
    with conn.cursor() as cur:
        if group_id:
            cur.execute("SELECT * FROM data_source_groups WHERE data_source_id = {};".format(int(ds_id)))
            res = cur.fetchall()
            logging.info(res)

def check_if_datasource_already_in_group(dsid, groupid):
    conn = open_conn()
    with conn.cursor() as cur:
        if groupid:
            cur.execute("SELECT id FROM data_source_groups WHERE data_source_id = {} AND group_id = {};".format(int(dsid), int(groupid)))
            res = cur.fetchone()
            if res:
                return res[0]
            return None
        else:
            logging.error("Cannot add datasource if group is None")
    return None

def add_datasource_to_group(ds_id, group_id=None):
    id = check_if_datasource_already_in_group(ds_id, group_id)
    if not id:
        conn = open_conn()
        with conn.cursor() as cur:
            if group_id:
                cur.execute("INSERT INTO data_source_groups (data_source_id, group_id, view_only) VALUES ({}, {}, 'f') "
                            "RETURNING id;".format(int(group_id), int(ds_id)))
                res = cur.fetchone()
                if res:
                    id = res[0]
    return id


def create_group(area=None):
    group_id = check_if_group_exists(area)
    if not group_id:
        conn = open_conn()
        with conn.cursor() as cur:
            if area:
                cur.execute("INSERT INTO groups (org_id, type, name, permissions, created_at) "
                             "VALUES ((SELECT id FROM organizations LIMIT 1), 'regular', '{}', '{}', now()) RETURNING id;"
                             .format(area, '{create_dashboard,create_query,edit_dashboard,edit_query,view_query,view_source,'
                                           'execute_query,list_users,schedule_query,list_dashboards,list_alerts,list_data_sources}'))
                res = cur.fetchone()
                group_id = res[0]
        conn.close()
    return group_id



def check_if_group_exists(group_name=None):
    conn = open_conn()
    with conn.cursor() as cur:
        if group_name:
            cur.execute("SELECT id FROM groups WHERE name ilike '{}';".format(group_name))
            res = cur.fetchone()
        elif args.name:
            cur.execute("SELECT id FROM groups where name ilike '{}';".format(args.name))
            res = cur.fetchone()
        else:
            logging.info("No hay ninguna variable de grupo para comparar")
    conn.close()
    if res:
        return res[0]
    return None


def add_microservice(amicroservice=None):
    global microservice
    if amicroservice:
        microservice = amicroservice
    if not microservice:
        logging.error("Only microservice names are supported")
        sys.exit(404)
    ms_data = pull_microservice_credentials()
    if ms_data:
        l = ['area', 'team', 'tier', 'vertical']
        engines = set(ms_data) - set(l)
        for item in engines:
            print("")
            new_dict = dict()
            if 'vertical' in ms_data.keys():
                new_dict['area'] = ms_data['vertical']
                del ms_data['vertical']
            if 'area' in ms_data.keys():
                new_dict['area'] = ms_data['area']
            if 'team' in ms_data.keys():
                new_dict['team'] = ms_data['team']
            if 'tier' in ms_data.keys():
                new_dict['tier'] = ms_data['tier']
            new_dict['type'] = item
            new_dict.update(ms_data[item])
            ds_data = create_datasource(new_dict)
            for group_name in ['area', 'team']:
                if ds_data:
                    if group_name in ds_data['data'].keys():
                        if ds_data['data'][group_name]:
                            group_id = create_group(ds_data['data'][group_name])
                            logging.info("Group Name: {}".format(ds_data['data'][group_name]))
                            if group_id:
                                logging.info("Adding datasource '{}' to group: '{}' if not exists".format(ds_data['res']['id'], group_id))
                                add_datasource_to_group(group_id, ds_data['res']['id'])
            logging.info("Checking if the ms is in another engine")
    else:
        logging.error("No available resources to insert in redash")


def add_all_microservices():
    exporter = EngineExporter(get_engine(), p_country)
    l = os.listdir(exporter.get_req_repo_path())
    li = [x.split('.')[0] for x in l]
    for ms in li:
        add_microservice(ms)


def execute_action():
    """
    Perform action
    """
    if p_action == "create-datasource":
        create_datasource()
    elif p_action == "get-datasource":
        get_datasource()
    elif p_action == "delete-datasource":
        delete_datasource()
    elif p_action == "get-all-datasources":
        get_all_datasources()
    elif p_action == "get-user":
        get_user()
    elif p_action == "get-all-users":
        get_all_users()
    elif p_action == "get-query":
        get_query()
    elif p_action == "delete-query":
        delete_query()
    elif p_action == "create-query":
        create_query()
    elif p_action == "refresh-query":
        refresh_query()
    elif p_action == "get-all-queries":
        get_all_queries()
    elif p_action == "query-and-wait-result":
        execute_query_and_wait()
    elif p_action == "add-microservice-by-database-host":
        if not p_database or not p_host:
            logging.error("Database and host needed")
            sys.exit(404)
        elif p_database.lower() == 'grability':
            logging.error("Database grability not supported")
            sys.exit(499)
        ms_data = pull_microservice_credentials(False)
        l = ['area', 'team', 'tier', 'vertical']
        engines = set(ms_data) - set(l)
        for item in engines:
            print("")
            new_dict = dict()
            if 'vertical' in ms_data.keys():
                new_dict['area'] = ms_data['vertical']
            if 'area' in ms_data.keys():
                new_dict['area'] = ms_data['area']
            if 'team' in ms_data.keys():
                new_dict['team'] = ms_data['team']
            if 'tier' in ms_data.keys():
                new_dict['tier'] = ms_data['tier']
            new_dict['type'] = item
            new_dict.update(ms_data[item])
            create_datasource(new_dict)
            logging.info("Checking if there is another engine")
        logging.info("The End")
    elif p_action == "add-microservice":
        add_microservice()
    elif p_action == "add-all-microservices":
        add_all_microservices()
    elif p_action == "create-group":
        create_group("growth")


    # elif p_action == "get-query-result":
    #    get_query_result()
    # elif p_action == "get-dashboard":
    #    get_dashboard()
    # elif p_action == "get-all-dashboards":
    #    get_all_dashboards()
    else:
        print("There are no actions with this name")


def main():
    execute_action()


if __name__ == '__main__':
    main()

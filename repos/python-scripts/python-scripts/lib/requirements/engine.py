from __future__ import annotations
import os
import sys
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/redash/common')
from EndpointConnectivity import EndpointConnectivity
import json
from pprint import pprint
import socket
import psycopg2
from pprint import pprint
import mysql.connector
import pyodbc
import urllib
import pymongo
import pandas as pd
import traceback
import dpath.util
import yaml
import logging

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.basicConfig(
    format='%(asctime)-2s %(name)-2s %(levelname)-4s: %(message)s',
    level=logging.INFO, #Nivel de los eventos que se registran en el logger
)


def check_dict_path(d, *indices):
    sentinel = object()
    for index in indices:
        d = d.get(index, sentinel)
        if d is sentinel:
            return False
    return True

possible_types = ['postgres','mongodb','mysql']

def get_instance_by_type(p_type, p_country):
    if p_type == "postgres":
        return PostgreSQLExporter(EngineExporter, p_country)
    elif p_type == "mysql":
        return MySQLExporter(EngineExporter, p_country)
    elif p_type == "mongodb" or p_type == "documentdb":
        return MongoDBExporter(EngineExporter, p_country)
    elif p_type == "sqlserver":
        return MsSQLExporter(EngineExporter, p_country)
    elif p_type == "aurora-mysql":
        return MySQLExporter(EngineExporter, p_country)
    elif p_type == "mssql":
        return MsSQLExporter(EngineExporter, p_country)
    elif p_type == "aurora":
        return MySQLExporter(EngineExporter, p_country)
    elif p_type == "aurora-postgres":
        return PostgreSQLExporter(EngineExporter, p_country)
    elif p_type == "rds_mysql":
        return MySQLExporter(EngineExporter, p_country)
    else:
        return None


class EngineExporter:
    """
    The Facade class provides a simple interface to the complex logic of one or
    several subsystems. The Facade delegates the client requests to the
    appropriate objects within the subsystem. The Facade is also responsible for
    managing their lifecycle. All of this shields the client from the undesired
    complexity of the subsystem.
    """

    def __init__(self, p_type, p_country) -> None:
        """
        Depending on your application's needs, you can provide the Facade with
        existing subsystem objects or force the Facade to create them on its
        own.
        """
        if p_type == "documentdb":
            p_type = "mongodb"
        self._type_description = p_type
        self._country = str(p_country).lower()
        self._type = get_instance_by_type(p_type, p_country)

    def get_options(self, database, host, user, secret, port="") -> dict:
            options = {}
            options['database'] = database
            options['host'] = host
            options['role'] = user
            options['password'] = secret
            options['port'] = port

            return options

    def pull_req_repo(self):
        home = os.path.expanduser("~")
        path = home + '/repos/devops-requirements/ms-requirements/'
        os.popen('git -C ' + path + ' pull > /dev/null 2>&1').read()

    def get_req_repo_path(self):
        home = os.path.expanduser("~")
        return str(home + '/repos/devops-requirements/ms-requirements/')

    def pull_microservice_credentials(self, microservice_name):
        """
        Genera el objeto del microservicio
        """
        if self._type_description == "all":
            res = dict()
            for atype in possible_types:
                instance = get_instance_by_type(atype, self._country)
                res[atype] = instance.pull_microservice_credentials(microservice_name)
            return res
        else:
            return {self._type_description: self._type.pull_microservice_credentials(microservice_name)}

    def check_connectivity(self, p_host, p_port):
        try:
            connectivity = EndpointConnectivity(p_host, p_port)
            connectivity.check_conn()
        except Exception as e:
            pprint("Error reaching server:" + str(e))
            pprint(traceback.format_exc())
            if "conn" in locals():
                conn.close()
            sys.exit(1)


    def extract_ms_credentials(self, ms_vars2, engine, omit_pass=False):
        """
        Genera las variables del conector source
        """
        if self._type_description == "all":
                instance = get_instance_by_type(engine, self._country)
                return instance.extract_ms_credentials(ms_vars2, omit_pass)
        else:
            return self._type.extract_ms_credentials(ms_vars2, omit_pass)

    def extract_ms_credentials_repset(self, ms_vars2, engine, omit_pass=False):
        instance = get_instance_by_type('mongodb', self._country)
        return instance.extract_ms_credentials_repset(ms_vars2, omit_pass)

    @property
    def get_type(self):
        return self._type


class PostgreSQLExporter(EngineExporter):
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """
    def __init__(self, p_type, p_country):
        super().__init__(p_type, p_country)

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=5432) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['dbname'] = p_database
        options['host'] = p_host
        options['user'] = p_user
        options['password'] = p_secret
        options['port'] = p_port

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            super().check_connectivity(p_host, p_port)

            conn = psycopg2.connect(
                dbname=p_database, user=p_user, port=p_port, host=p_host, password=p_secret)
            print("Connection with credentials: OK")
            conn.close()
        except psycopg2.Error as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def generate_var_values(self, vars):
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

    def pull_microservice_credentials(self, microservice_name):
        """
        Genera el objeto del microservicio
        """
        try:
            conf_list = dict()
            conf_list['original'] = {}
            conf_list['original_read'] = {}
            conf_list['external'] = {}
            conf_list['monolith'] = {}
            conf_list['extra'] = {}
            home = os.path.expanduser("~")
            database = None
            if isinstance(microservice_name, str):
                var_microservice = microservice_name
            elif isinstance(microservice_name, dict):
                var_microservice = microservice_name['ms']
                database = microservice_name['database_var']
            super().pull_req_repo()
            with open(super().get_req_repo_path() + var_microservice + '.yml') as file:
                list_vars = yaml.load(file, Loader=yaml.FullLoader)
                if check_dict_path(list_vars, "resources", 'postgres'):
                    for each_config in dpath.util.get(list_vars, "resources/postgres"):
                        if each_config['var_map']['anansiblevar_database_name_var'] == database or database is None:
                            role_vars = self.generate_var_values(each_config['var_map'])
                            if role_vars:
                                if "_read" in each_config['var_map'].keys():
                                    conf_list['original_read'] = role_vars
                                else:
                                    conf_list['original'] = role_vars
                if check_dict_path(list_vars, "resources", 'monolith_database_access'):
                    whole_data = dpath.util.get(list_vars, "resources/monolith_database_access")
                    for skip_ms in whole_data.keys():
                        if whole_data[skip_ms]['var_map']['anansiblevar_database_name_var'] == database or database is None:
                            role_vars = self.generate_var_values(whole_data[skip_ms]['var_map'])
                            if role_vars:
                                conf_list['monolith'] = role_vars
                if check_dict_path(list_vars, "requirements", 'new_postgres_database'):
                    whole_data = dpath.util.get(list_vars, "requirements/new_postgres_database")
                    for skip_ms in whole_data.keys():
                        if whole_data[skip_ms]['var_map']['anansiblevar_database_name_var'] == database or database is None:
                            role_vars = self.generate_var_values(whole_data[skip_ms]['var_map'])
                            if role_vars:
                                if "_read" in whole_data[skip_ms]['var_map'].keys():
                                    conf_list['original_read'] = role_vars
                                else:
                                    conf_list['original'] = role_vars
                if check_dict_path(list_vars, "requirements", 'monolith_database_access'):
                    whole_data = dpath.util.get(list_vars, "requirements/monolith_database_access")
                    for skip_ms in whole_data.keys():
                        if whole_data[skip_ms]['var_map']['anansiblevar_database_name_var'] == database or database is None:
                            role_vars = self.generate_var_values(whole_data[skip_ms]['var_map'])
                            if role_vars:
                                conf_list['monolith'] = role_vars
                if check_dict_path(list_vars, "resources", 'ms_database_access'):
                    for each_data in dpath.util.get(list_vars, "resources/ms_database_access"):
                        if each_data['var_map']['anansiblevar_database_name_var'] == database or database is None:
                            role_vars = self.generate_var_values(each_data['var_map'])
                            if role_vars:
                                conf_list['external'] = role_vars
                if check_dict_path(list_vars, "service_metadata"):
                    if conf_list:
                        conf_list.update(dpath.util.get(list_vars, "service_metadata"))
            return conf_list
        except Exception as e:
            pprint("Error getting microservice vars from requirements:" + str(e))
            sys.exit(1)

    def extract_ms_credentials(self, ms_vars2, omit_pass=False):
        try:
            conf = dict()
            if isinstance(ms_vars2, dict):
                for key in ['var_database', 'var_role', 'var_host', 'var_port', 'var_password']:
                    if "var_password" not in ms_vars2.keys() and "var_password_url" in ms_vars2.keys():
                        ms_vars2["var_password"] = ms_vars2["var_password_url"]
                    result = os.popen('python3 ' + git_root +
                                      '/repos/python-scripts/dbacli variables get_variable --name '
                                      + ms_vars2[key] + ' --country ' + self._country).read()
                    if result.strip():
                        conf[key.replace("var_", "")] = json.loads(result)['value']
                    elif key == "var_password":
                        result = os.popen('python3 ' + git_root +
                                          '/repos/python-scripts/dbacli variables get_variable --name '
                                          + ms_vars2[key + '_url'] + ' --country ' + self._country).read()
                        conf[key.replace("var_", "")] = json.loads(result)['value']
                    else:
                        return dict()
                if omit_pass:
                    del conf['role']
                    del conf['password']
            return conf
        except Exception as e:
            logging.error("Error extracting ms credentials:" + str(e))
            logging.error(traceback.format_exc())
            sys.exit(1)


class MySQLExporter(EngineExporter):
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """
    def __init__(self, p_type, p_country):
        super().__init__(p_type, p_country)

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=3306) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['db'] = p_database
        options['passwd'] = p_secret
        options['port'] = p_port
        options['host'] = p_host
        options['user'] = p_user

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            super().check_connectivity(p_host, p_port)

            conn = mysql.connector.connect(
                database=p_database, user=p_user, port=p_port, host=p_host, password=p_secret)
            print("Connection with credentials: OK")
            conn.close()
        except mysql.connector.Error as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def generate_var_values(self, vars):
        """
        Armo las vars
        """
        conf = dict()
        conf['var_host'] = vars['anansiblevar_mysql_host_var']
        conf['var_database'] = vars['anansiblevar_mysql_database_var']
        if "anansiblevar_mysql_password_var" in vars.keys():
            conf['var_password'] = vars['anansiblevar_mysql_password_var']
            if "_url" == vars['anansiblevar_mysql_password_var'][-4:]:
                conf['var_password'] = vars['anansiblevar_mysql_password_var'].replace("_url", "")
        if "anansiblevar_mysql_password_var_url" in vars.keys():
            conf['var_password_url'] = vars['anansiblevar_mysql_password_var_url']
        conf['var_port'] = vars['anansiblevar_mysql_port_var']
        conf['var_role'] = vars['anansiblevar_mysql_user_var']
        return conf

    def pull_microservice_credentials(self, microservice_name):
        """
        Genera el objeto del microservicio
        """
        try:
            conf_list = dict()
            conf_list['original'] = {}
            conf_list['original_read'] = {}
            conf_list['external'] = {}
            conf_list['monolith'] = {}
            conf_list['extra'] = {}
            home = os.path.expanduser("~")
            database = None
            if isinstance(microservice_name, str):
                var_microservice = microservice_name
            elif isinstance(microservice_name, dict):
                var_microservice = microservice_name['ms']
                database = microservice_name['database_var']
            super().pull_req_repo()
            with open(super().get_req_repo_path() + var_microservice + '.yml') as file:
                list_vars = yaml.load(file, Loader=yaml.FullLoader)
                if check_dict_path(list_vars, "resources", 'mysql'):
                    for each_config in dpath.util.get(list_vars, "resources/mysql"):
                        if each_config['var_map']['anansiblevar_mysql_database_var'] == database or database is None:
                            role_vars = self.generate_var_values(each_config['var_map'])
                            if role_vars:
                                if "_read" in each_config['var_map'].keys():
                                    conf_list['original_read'] = role_vars
                                else:
                                    conf_list['original'] = role_vars
                if check_dict_path(list_vars, "service_metadata"):
                    if conf_list:
                        conf_list.update(dpath.util.get(list_vars, "service_metadata"))
            return conf_list
        except Exception as e:
            pprint("Error getting microservice vars from requirements:" + str(e))
            sys.exit(1)

    def extract_ms_credentials(self, ms_vars2, omit_pass=False):
        try:
            conf = dict()
            if isinstance(ms_vars2, dict):
                for key in ['var_database', 'var_role', 'var_host', 'var_port', 'var_password']:
                    if "var_password" not in ms_vars2.keys() and "var_password_url" in ms_vars2.keys():
                        ms_vars2["var_password"] = ms_vars2["var_password_url"]
                    result = os.popen('python3 ' + git_root +
                                      '/repos/python-scripts/dbacli variables get_variable --name '
                                      + ms_vars2[key] + ' --country ' + self._country).read()
                    if result.strip():
                        conf[key.replace("var_", "")] = json.loads(result)['value']
                    elif key == "var_password":
                        result = os.popen('python3 ' + git_root +
                                          '/repos/python-scripts/dbacli variables get_variable --name '
                                          + ms_vars2[key + '_url'] + ' --country ' + self._country).read()
                        conf[key.replace("var_", "")] = json.loads(result)['value']
                    else:
                        return dict()
                if omit_pass:
                    del conf['role']
                    del conf['password']
            return conf
        except Exception as e:
            logging.error("Error extracting ms credentials:" + str(e))
            logging.error(traceback.format_exc())
            sys.exit(1)


class MsSQLExporter(EngineExporter):
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """
    def __init__(self, p_type, p_country):
        super().__init__(p_type, p_country)

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=1433, charset="UTF-8", tds_version="7.0") -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['charset'] = charset
        options['db'] = p_database
        options['password'] = p_secret
        options['port'] = p_port
        options['server'] = p_host
        options['tds_version'] = tds_version
        options['user'] = p_user

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            super().check_connectivity(p_host, p_port)

            conn_string = ("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format(
                "{ODBC Driver 17 for SQL Server}", p_host, p_database, p_user, urllib.parse.quote(p_secret)))
            conn = pyodbc.connect(conn_string)
            print("Connection with credentials: OK")
            conn.close()
        except Exception as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def extract_ms_credentials(self, ms_vars2):
        super().extract_ms_credentials(ms_vars2)


class MongoDBExporter(EngineExporter):
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """
    def __init__(self, p_type, p_country ):
        super().__init__(p_type, p_country)

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=27017) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['connectionString'] = 'mongodb://{}:{}@{}:{}'.format(p_user, urllib.parse.quote(p_secret), p_host, p_port)
        options['dbName'] = p_database

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            super().check_connectivity(p_host, p_port)

            pprint(p_host, p_port)

            conn_string = 'mongodb://{}:{}@{}:{}'.format(p_user, urllib.parse.quote(p_secret), p_host, p_port)
            conn = pymongo.MongoClient(conn_string, serverSelectionTimeoutMS=3000)
            conn.server_info()
            print("Connection with credentials: OK")
            conn.close()
        except Exception as e:
            pprint("Error connecting to database:" + str(e))
            pprint(traceback.format_exc())
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def generate_var_values(self, vars):
        """
        Armo las vars
        """
        conf = dict()
        conf['var_writer'] = vars['anansiblevar_mongo_conn_string']
        if "_read" in vars.keys():
            conf['var_reader'] = vars['anansiblevar_mongo_conn_string_read']
        return conf

    def generate_repset_var_values(self, vars):
        """
        Armo las vars
        """
        conf = dict()
        conf['var_host'] = vars['anansiblevar_mongo_host_var']
        conf['var_database'] = vars['anansiblevar_mongo_database_var']
        if "anansiblevar_mongo_password_var" in vars.keys():
            conf['var_password'] = vars['anansiblevar_mongo_password_var']
            if "_url" == vars['anansiblevar_mongo_password_var'][-4:]:
                conf['var_password'] = vars['anansiblevar_mongo_password_var'].replace("_url", "")
        if "anansiblevar_mongo_password_var_url" in vars.keys():
            conf['var_password_url'] = vars['anansiblevar_mongo_password_var_url']
        conf['var_port'] = 27017
        conf['var_role'] = vars['anansiblevar_mongo_user_var']
        return conf

    def pull_microservice_credentials(self, microservice_name):
        """
        Genera el objeto del microservicio
        """
        try:
            conf_list = dict()
            conf_list['original'] = {}
            conf_list['original_read'] = {}
            conf_list['external'] = {}
            conf_list['monolith'] = {}
            conf_list['extra'] = {}
            home = os.path.expanduser("~")
            database = None
            if isinstance(microservice_name, str):
                var_microservice = microservice_name
            elif isinstance(microservice_name, dict):
                var_microservice = microservice_name['ms']
                database = microservice_name['database_var']
            super().pull_req_repo()
            with open(super().get_req_repo_path() + var_microservice + '.yml') as file:
                list_vars = yaml.load(file, Loader=yaml.FullLoader)
                if check_dict_path(list_vars, "resources", 'documentdb'):
                    for each_config in dpath.util.get(list_vars, "resources/documentdb"):
                        if 'repset' in each_config.keys() or "anansiblevar_mongo_database_var" in each_config['var_map'].keys():
                            if each_config['var_map']['anansiblevar_mongo_database_var'] == database or database is None:
                                role_vars = self.generate_repset_var_values(each_config['var_map'])
                                if role_vars:
                                    role_vars['repset'] = True
                                    conf_list['original'] = role_vars
                        else:
                            if str(database) in each_config['var_map']['anansiblevar_mongo_conn_string'] or database is None:
                                role_vars = self.generate_var_values(each_config['var_map'])
                                if role_vars:
                                    if "_read" in each_config['var_map'].keys():
                                        conf_list['original_read'] = {'conn_string': role_vars['var_reader']}
                                    conf_list['original'] = {'conn_string': role_vars['var_writer']}
                if check_dict_path(list_vars, "service_metadata"):
                    if conf_list:
                        conf_list.update(dpath.util.get(list_vars, "service_metadata"))
            return conf_list
        except Exception as e:
            pprint("Error getting microservice vars from requirements:" + str(e))
            sys.exit(1)

    def transform_data(self, var_keys):
        conf = dict()
        result = os.popen('python3 ' + git_root +
                          '/repos/python-scripts/dbacli variables get_variable --name '
                          + var_keys['conn_string'] + ' --country ' + self._country).read()
        if result.strip():
            res = json.loads(result)['value']
            if res:
                main_split = res.split("@")
                conf['role'], conf['password'] = str(main_split[0].replace("mongodb://", "").strip()).split(":")
                if "," in main_split[1]:
                    conf['host'], conf['database'] = str(main_split[1]).split("/")
                    conf['host'], conf['port'] = str(conf['host'].split(",")[0]).split(":")
                    conf['database'] = conf['database'].split("?")[0].strip()
                else:
                    conf['host'], conf['database'] = str(main_split[1]).split("/")
                    conf['database'] = conf['database'].split("?")[0].strip()
                    conf['host'], conf['port'] = conf['host'].split(":")
        return conf

    def extract_ms_credentials(self, ms_vars2, omit_pass=False):
        """
        Genera las variables del conector source
        """
        try:
            conf = dict()
            if isinstance(ms_vars2, dict):
                if "conn_string" in ms_vars2.keys():
                    conf = self.transform_data(ms_vars2)
                    if omit_pass:
                        del conf['role']
                        del conf['password']
            return conf
        except Exception as e:
            pprint("Error extracting ms credentials:" + str(e))
            pprint(traceback.format_exc())
            sys.exit(1)

    def extract_ms_credentials_repset(self, ms_vars2, omit_pass=False):
        """
        Genera las variables del conector source
        """
        try:
            conf = dict()
            if isinstance(ms_vars2, dict):
                for key in ['var_database', 'var_role', 'var_host', 'var_password']:
                    if "var_password" not in ms_vars2.keys() and "var_password_url" in ms_vars2.keys():
                        ms_vars2["var_password"] = ms_vars2["var_password_url"]
                    result = os.popen('python3 ' + git_root +
                                      '/repos/python-scripts/dbacli variables get_variable --name '
                                      + ms_vars2[key] + ' --country ' + self._country).read()
                    if result.strip():
                        conf[key.replace("var_", "")] = json.loads(result)['value']
                    elif key == "var_password":
                        result = os.popen('python3 ' + git_root +
                                          '/repos/python-scripts/dbacli variables get_variable --name '
                                          + ms_vars2[key + '_url'] + ' --country ' + self._country).read()
                        conf[key.replace("var_", "")] = json.loads(result)['value']
                if omit_pass:
                    del conf['role']
                    del conf['password']
                conf['port'] = ms_vars2['var_port']
            return conf
        except Exception as e:
            pprint("Error extracting ms credentials:" + str(e))
            pprint(traceback.format_exc())
            sys.exit(1)

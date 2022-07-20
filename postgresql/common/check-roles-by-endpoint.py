#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Get roles related to a database/endpoint.
# Collect vital stats when needed

import os
import json
import subprocess
from pprint import pprint
import sys
import argparse
import shelve
import time
import yaml
import dpath.util
from shlex import quote as shlex_quote

os.system('clear')

parser = argparse.ArgumentParser(description='Check and store PostgreSQL Database Roles in RDS')

parser.add_argument('-c', '--country', action="store", dest="country",
                    help="Country -> [ar,br,cl,co,cr,ec,xx1,x1,xx1,dev,uy]", required=True)
parser.add_argument('-e', '--endpoint', action="store", dest="source", help="AWS Instance Endpoint", required=True)
parser.add_argument('-d', '--database', action="store", dest="database", help="Database Name",
                    required=True)

args = parser.parse_args()
country = args.country
source = args.source
database = args.database

git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]

try:
    source_endpoint = os.popen("nslookup " + source + " | grep Name | cut -d: -f2 | xargs").readline().strip()
except Exception as e:
    pprint("Error checking source endpoint" + str(e))
    sys.exit(1)


def checkRoles():
    """
  Gets roles from shelve or examines them on demand
  """
    microdict = getDatabaseRegisteredVars()
    if microdict:
        pprint(microdict)
    else:
        generateShelveRoles()
        pprint(getDatabaseRegisteredVars())



def generateShelveRoles():
    """
    Genera las variables del conector y los microservicios que acceden
    """
    #pprint("generateShelveRoles")
    try:
        msvars = getDatabaseRegisteredVars()

        d = shelve.open("/tmp/shelve_roles_" + country, writeback=True)
        if not msvars:
            msvarslist = []
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
                    ms_w_var = dict()
                    ms_w_var['ms'] = temp_result
                    ms_w_var['database_var'] = each_var
                    ms_names.append(ms_w_var)

            for each_ms in ms_names:
                ms_variables_list = pullMicroserviceCredentials(each_ms)
                for ms_variables in ms_variables_list:
                    temp = extractMsCredentials(ms_variables)
                    temp_endpoint = os.popen("nslookup " + temp['host'] +
                                             " | grep Name | cut -d: -f2 | xargs").readline().strip()
                    if str(temp_endpoint) == str(source_endpoint):
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
        d.close()
    except Exception as e:
        pprint("Error generating roles config:" + str(e))
        sys.exit(1)


def getDatabaseRegisteredVars():
    """
    Devuelve los roles asociados a un endpoint y database
    """
    #pprint("getDatabaseRegisteredVars")
    try:
        variables = dict()
        file_path = "/tmp/shelve_roles_" + country
        if os.path.isfile(file_path + ".db") or os.path.isfile(file_path):
            os.popen('sudo chmod 774 /tmp/shelve_roles_' + '*').readline().strip()
            #os.popen('sudo chown ubuntu:adm /tmp/shelve_roles_' + '*').readline().strip()
        d = shelve.open("/tmp/shelve_roles_" + country, writeback=True)
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


def generateVarValues(vars):
    """
    Armo las vars
    """
    conf = dict()
    conf['var_host'] = vars['anansiblevar_database_host_var']
    conf['var_database'] = vars['anansiblevar_database_name_var']
    if "anansiblevar_database_password_var" in vars.keys():
        conf['var_password'] = vars['anansiblevar_database_password_var']
    elif "anansiblevar_database_password_var_url" in vars.keys():
        conf['var_password'] = vars['anansiblevar_database_password_var_url']
    conf['var_port'] = vars['anansiblevar_database_port_var']
    conf['var_role'] = vars['anansiblevar_database_user_var']
    return conf


def check_dict_path(d, *indices):
    sentinel = object()
    for index in indices:
        d = d.get(index, sentinel)
        if d is sentinel:
            return False
    return True


def extractMsCredentials(ms_vars2):
    """
    Genera las variables del conector source
    """
    #pprint("extractMsCredentials")
    try:
        conf = dict()
        conf['database'] = json.loads(os.popen('python3 ' + git_root +
                                               '/repos/python-scripts/dbacli variables get_variable --name '
                                               + ms_vars2['var_database'] + ' --country '
                                               + country).read())['value']
        conf['role'] = json.loads(os.popen('python3 ' + git_root +
                                                   '/repos/python-scripts/dbacli variables get_variable --name '
                                                   + ms_vars2['var_role'] + ' --country '
                                                   + country).read())['value']
        conf['host'] = json.loads(os.popen('python3 ' + git_root +
                                                   '/repos/python-scripts/dbacli variables get_variable --name '
                                                   + ms_vars2['var_host'] + ' --country '
                                                   + country).read())['value']
        conf['port'] = json.loads(os.popen('python3 ' + git_root +
                                                   '/repos/python-scripts/dbacli variables get_variable --name '
                                                   + ms_vars2['var_port'] + ' --country '
                                                   + country).read())['value']
        conf['password'] = json.loads(os.popen('python3 ' + git_root +
                                                       '/repos/python-scripts/dbacli variables get_variable --name '
                                                       + ms_vars2['var_password'] + ' --country '
                                                       + country).read())['value']
        return conf
    except Exception as e:
        pprint("Error extracting ms credentials:" + str(e))
        sys.exit(1)


def main():
    checkRoles()


if __name__ == '__main__':
    main()

#!/usr/bin/python3

import os
import logging
import sys
import argparse
import dpath.util
import yaml
import json
from pprint import pprint
import pandas as pd
import traceback
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib.requirements.engine import EngineExporter


logging.basicConfig(filename='/tmp/getting-ms-requirements.log', filemode='w', level=logging.DEBUG)
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]

parser = argparse.ArgumentParser(description='Getting MS info')

parser.add_argument('-c', '--country', action="store", dest="country", help="AWS Instance Endpoint", required=True)
parser.add_argument('-m', '--microservice', action="store", dest="microservice", help="Microservices (comma "
                                                                                      "separated)",
                    required=False)
parser.add_argument('-d', '--database', action="store", dest="database", help="Databases (comma separated)",
                    required=False)
parser.add_argument('-e', '--engine', action="store", dest="engine", default="all",
                    help="Engine to search requirements")
parser.add_argument('--only-aeas', action="store_true", dest="global_info", help="Only show global info")
parser.add_argument('--omit-pass', action="store_true", dest="omit_pass", help="Only show host and port")

args = parser.parse_args()


country = str(args.country).lower()
engine = str(args.engine).lower()
if args.database is not None:
    arg_db = str(args.database).lower()
    list_dbs = [x.strip() for x in arg_db.split(',')]
if args.microservice is not None:
    arg_ms = str(args.microservice).lower()
    list_ms = [x.strip() for x in arg_ms.split(',')]

exporter = EngineExporter(engine, country)


def pull_repo():
    exporter.pull_req_repo()


def extract_ms_credentials(ms_vars2):
    """
    Genera las variables del conector source
    """
    try:
        conf = dict()
        if isinstance(ms_vars2, dict):
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
            if args.omit_pass:
                del conf['role']
                del conf['password']
        return conf
    except Exception as e:
        pprint("Error extracting ms credentials:" + str(e))
        pprint(traceback.format_exc())
        sys.exit(1)


def reformat_ms_list(database_list):
    new_list = []
    for each_data in database_list:
        microservices_vars = []
        microservices_vars = json.loads(os.popen('python3 ' + git_root +
                                                 '/repos/python-scripts/dbacli variables get_value --exact '
                                                 + each_data + ' --country ' + country).read())
        ids = [x['id'] for x in microservices_vars]
        ms_names = []
        for each_var in ids:
            home = os.path.expanduser("~")
            temp_result = os.popen('grep ' + each_var + ' ' + exporter.get_req_repo_path()
                                   + '* | cut -d: -f1 | sed "s/.yml//g" | awk -F/ \'{print $NF}\'')\
                .read().splitlines()  # .readline().strip()
            if temp_result is not None and temp_result:
                for i in range(0, len(temp_result)):
                    ms_w_var = dict()
                    ms_w_var['ms'] = temp_result[i]
                    ms_w_var['database_var'] = each_var
                    ms_names.append(ms_w_var)
        ms_names_no_dup = pd.DataFrame(ms_names).drop_duplicates().to_dict('records')
        new_list = new_list + ms_names_no_dup
    return new_list


def count_areas(dictio):
    aux = dictio
    if "areas" not in aux.keys():
        aux['areas'] = dict()
    for k in dictio.keys():
        if k != "areas":
            if 'vertical' in aux[k].keys():
                aux[k]['area'] = aux[k]['vertical']
                del aux[k]['vertical']
            if 'area' in aux[k].keys():
                if aux[k]['area'] in aux['areas'].keys():
                    aux['areas'][aux[k]['area']]['total'] += 1
                    if aux[k]['team'] in aux['areas'][aux[k]['area']].keys():
                        aux['areas'][aux[k]['area']][aux[k]['team']] += 1
                    else:
                        aux['areas'][aux[k]['area']][aux[k]['team']] = 1
                else:
                    aux['areas'][aux[k]['area']] = dict()
                    aux['areas'][aux[k]['area']]['total'] = 1
                    aux['areas'][aux[k]['area']][aux[k]['team']] = 1
    return aux


def main():
    ms_complete_list = dict()
    temp = dict()
    iter_list = []
    type_microservice = False
    if args.microservice:
        iter_list = list_ms
        type_microservice = True
    elif args.database:
        iter_list = list_dbs
    if not type_microservice:
        iter_list = reformat_ms_list(iter_list)
    for each_ms in iter_list:
        ms_variables_total = exporter.pull_microservice_credentials(each_ms)
        if isinstance(each_ms, str):
            ms = each_ms
        elif isinstance(each_ms, dict):
            ms = each_ms['ms']
        if ms_variables_total:
            if ms not in ms_complete_list.keys():
                ms_complete_list[ms] = dict()
            for ms_variable_engine in ms_variables_total.keys():
                ms_variables_dict = ms_variables_total[ms_variable_engine]
                if ms_variables_dict:
                    for ms_variable_key in ms_variables_dict.keys():
                        if not args.global_info:
                            if isinstance(ms_variables_dict[ms_variable_key], dict) and ms_variables_dict[ms_variable_key]:
                                if 'repset' in ms_variables_dict[ms_variable_key].keys():
                                    temp = exporter.extract_ms_credentials_repset(ms_variables_dict[ms_variable_key], ms_variable_engine, args.omit_pass)
                                else:
                                    temp = exporter.extract_ms_credentials(ms_variables_dict[ms_variable_key], ms_variable_engine, args.omit_pass)
                        else:
                            if isinstance(ms_variables_dict[ms_variable_key], dict) and ms_variables_dict[ms_variable_key]:
                                for k in ms_variables_dict[ms_variable_key].keys():
                                    if k in ['area', 'team', 'tier', 'vertical']:
                                        if ms_variable_key not in ms_complete_list[ms].keys():
                                            ms_complete_list[ms][ms_variable_key] = dict()
                                        ms_complete_list[ms][ms_variable_key][k] = ms_variables_dict[ms_variable_key][k]
                        if temp:
                            if ms not in ms_complete_list.keys():
                                ms_complete_list[ms] = dict()
                            if ms_variable_engine not in ms_complete_list[ms].keys():
                                ms_complete_list[ms][ms_variable_engine] = dict()
                            ms_complete_list[ms][ms_variable_engine][ms_variable_key] = temp
                        else:
                            for k in ms_variables_dict.keys():
                                if k in ['area', 'team', 'tier', 'vertical']:
                                    ms_complete_list[ms][k] = ms_variables_dict[k]
                        temp = None
    ms_complete_list = count_areas(ms_complete_list)
    no_data = True
    for each_ms in iter_list:
        if isinstance(each_ms, str):
            ms = each_ms
        elif isinstance(each_ms, dict):
            ms = each_ms['ms']
        comp_array = list(ms_complete_list[ms].keys())
        if 'vertical' in comp_array:
            comp_array.remove('vertical')
        if 'area' in comp_array:
            comp_array.remove('area')
        if 'team' in comp_array:
            comp_array.remove('team')
        if 'tier' in comp_array:
            comp_array.remove('tier')
        if len(comp_array) > 0:
            no_data = False
    if no_data and not args.global_info:
        print("No hay datos para los ms buscados en ese engine")
        sys.exit(500)
    if not args.global_info:
        print(json.dumps(ms_complete_list, indent=4, sort_keys=True))
    else:
        print(json.dumps(ms_complete_list['areas'], indent=4, sort_keys=True))


if __name__ == '__main__':
    main()
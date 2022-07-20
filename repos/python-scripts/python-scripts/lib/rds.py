#!/usr/bin/python3
import dbm
import os
import re
import shelve
import shutil
import subprocess
import sys
import time
import boto3
import psycopg2
import urllib.parse
from .settings import Settings as s
from bson import json_util
import json
from botocore.exceptions import ClientError
from inspect import getmembers
from pprint import pprint
from .dynamo_rds import DynamoRDSClient
from .dynamo import DynamoClient
import traceback
from tabulate import tabulate

class RDSClient:

    def __init__(self, country):
        self.country = country.lower()
        self.region = s.REGION[self.country]
        self.session = boto3.session.Session(profile_name=s.AWS_PROFILE[self.country])
        self.infra_client = self.session.client('rds', region_name=self.region)

    def get_rds_instance_description(self, rds_instance_id):
        return self.infra_client.describe_db_instances(
            InstanceIds=[
                rds_instance_id
            ]
        )

    def get_rds_all_instances(self, engine=''):
        if engine:
            filter_list = []
            filter_list.append({'Name': 'engine', 'Values': [engine]})
            return self.infra_client.describe_db_instances(Filters=filter_list)
        return self.infra_client.describe_db_instances()

    def search_instances(self, instance_name, engine='', throttle=0.2):
        try:
            if engine:
                filter_list = []
                filter_list.append({'Name': 'engine', 'Values': [engine]})
                result = self.infra_client.describe_db_instances(DBInstanceIdentifier=instance_name, Filters=filter_list)
            else:
                result = self.infra_client.describe_db_instances(DBInstanceIdentifier=instance_name)
            return result["DBInstances"]
        except Exception as e:
            if "Throttling" in str(e):
                time.sleep(throttle)
                return self.search_instances(instance_name, engine, throttle * 2)
            else:
                if "DBInstanceNotFound" in str(e):
                    pprint("Deleting unknown instance from dynamo:" + str(e))
                    sys.exit(1)
                else:
                    pprint("Error getting instance data:" + str(e))
                    sys.exit(1)

    def __eqprof__(self, other):
        if not isinstance(other, RDSClient):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return s.AWS_PROFILE[self.country] == s.AWS_PROFILE[other.country]

    def getAwsInstanceName(instance):
        dbinstancelist = instance.split("-")
        dbinstancelist = dbinstancelist[:-4]
        dbinstance = "-".join(dbinstancelist)
        return dbinstance

    def getAwsAccount(instance):
        account = instance.split("-")
        return account[-1]

    def getAwsRegion(instance):
        region = instance.split("-")
        region = region[-4:-1]
        region = "-".join(region)
        return region

    def apply_tag(self, inst, tag):

        raw_tag = tag.split("=")
        tag=[]
        tag['Key'] = raw_tag[1]
        tag['Value'] = raw_tag[2]

    def search_instances_to_tag(self, pages, tag, family):
        """
      Get list of all the DBs from RDS
      """

        for page in pages:
            for i in page["DBInstances"]:
                if family == "replica" & str(i["ReadReplicaSourceDBInstanceIdentifier"]).count() > 1:
                    self.apply_tag(i["DBInstanceIdentifier"], tag)
                elif family == "master" & ("ReadReplicaDBInstanceIdentifiers" in i.keys()):
                    self.apply_tag(i["DBInstanceIdentifier"], tag)
                elif family == "all":
                    self.apply_tag(i["DBInstanceIdentifier"], tag)

    def search_all_instances(self, var_filter="", throttle=0.2):
        """
      Get list of all the DBs from RDS
      """
        try:
            paginator = self.infra_client.get_paginator('describe_db_instances')
            if var_filter:
                return paginator.paginate(Filters=var_filter)
            return paginator.paginate()
        except Exception as e:
            if "Throttling" in str(e):
                time.sleep(throttle)
                return self.search_all_instances(var_filter, throttle * 2)
            else:
                pprint("Error searching all instances:" + str(e))
                sys.exit(1)

    def apply_tag_in_instances(self, instance, tag, engine, family):
        if instance == "all":
            self.search_instances_to_tag(self.search_all_instances(self), tag, engine, family)
        else:
            self.apply_tag(str(self.search_instances(instance)["DBInstanceIdentifier"]), tag)

        return "Finish"

    def get_docker_image(self, version):
        """
        Retorna el docker image segun la version de postgres del conector
        """
        return os.popen('sudo docker ps --format "{{.Image}}" | grep ' + version.split(".")[0] + '.').read().splitlines()[0]

    def get_docker_name(self, version):
        """
        Retorna el docker image segun la version de postgres del conector
        """
        return os.popen('sudo docker ps --format "{{.Names}}" | grep ' + version.split(".")[0] + '.').read().splitlines()[0]

    def generate_shelve_admin_users(self):
        """
        Genera un shelve file con los admin users ubicados en pgpass.
        """
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]

        if not os.path.isfile("/tmp/svpass"):
            if not os.path.isfile(git_root + "/.pgpass"):
                shutil.copy2("~/.pgpass", git_root + "/.pgpass")
            file = open(git_root + "/.pgpass", "r").read().splitlines()

            d = shelve.open("/tmp/svpass", writeback=True)
            for line in file:
                array = str(line).split(":")
                if array[3] != "postgres":
                    d[array[3]] = array[4]
            d.close()

    def get_user_secret(self, user):
        """
        Devuelve la password del usuario de replicacion
        """
        if user == "dba_test_service":
            try:
                i_dynamo = DynamoClient('dev')
                var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
                return var['value']
            except Exception as e:
                print("No se encontraron variables del user en dynamo dev, busco en prod")
                i_dynamo = DynamoClient('xx')
                var = json.loads(i_dynamo.get_vars('dba_test_service_password'))
                return var['value']

    def find_in_shelve(self, instance_name, engine, country=""):
        d = shelve.open("/tmp/shelve_rds_conn")
        key = str(self.country) + "-" + instance_name


        #for key in d.keys():
        #    pprint(key)
        #    print(json.loads(d[key], object_hook=json_util.object_hook))
        if key in d.keys():
            idata = d[key]
        d.close()
        try:
            if "idata" in locals():
                if "last_check" in idata.keys():
                    if time.time() - idata['last_check'] < 604800:
                        return idata
                    else:
                        self.add_instance_in_shelve(instance_name, engine)
                        return self.find_in_shelve(instance_name, engine)
                else:
                    idata["last_check"] = time.time()
                    self.save_data_in_shelve_key("/tmp/shelve_rds_conn", key, idata)
                return idata
            else:
                self.add_instance_in_shelve(instance_name, engine)
                return self.find_in_shelve(instance_name, engine)


        except Exception as e:
            pprint("Error opening shelve file:" + str(e))
            sys.exit(1)

    def find_in_dynamo(self, instance_name, engine="postgres"):

        dynamordscli = DynamoRDSClient(self.country)
        key = dynamordscli.get_vars(instance_name)
        try:
            if key:
                idata = json.loads(key['value'])
                if "idata" in locals():
                    if "last_check" in idata.keys():
                        if time.time() - idata['last_check'] < 604800:
                            if "inventory" not in idata.keys():
                                inventory_data = self.generate_inv_data(idata, self.country)
                                self.add_instance_inventory_dynamo(inventory_data)
                                return self.find_in_dynamo(instance_name, engine)
                            else:
                                return idata
                        else:
                            inventory_data = self.generate_inv_data(idata, self.country)
                            self.add_instance_inventory_dynamo(inventory_data)
                            return self.find_in_dynamo(instance_name, engine)
                    else:
                        idata["last_check"] = time.time()
                        self.save_data_in_dynamo_key(key["id"], idata)
                    return idata
            else:
                self.add_instance_in_dynamo(instance_name, engine)
                return self.find_in_dynamo(instance_name, engine)
        except Exception as e:
            pprint("Error getting info from instance in dynamo:" + str(e))
            sys.exit(1)

    def save_data_in_shelve_key(self, file, key, data):
        try:
            with shelve.open(str(file)) as d:
                d[key] = data
        except Exception as e:
            pprint("Error saving data in bin file:" + str(e))

    def save_data_in_dynamo_key(self, key, data):
        try:
            dynamordscli = DynamoRDSClient(self.country)
            dynamordscli.update_vars(str(key), json.dumps(data, default=json_util.default))
        except Exception as e:
            pprint("Error saving data in dynamo:" + str(e))

    def connect_shelve(self, instance_name, engine, database, user, secret):
        conninfo = self.find_in_shelve(instance_name, engine)
        if not secret:
            secret = self.get_user_secret(user)
        try:
            cmd = "sudo docker run --rm -i -t {} sh -c \"psql postgresql://{}:{}@{}:{}/{}\"".format(self.get_docker_image(
                conninfo['EngineVersion']), user, re.escape(secret), conninfo['Endpoint']['Address'], "5432", database)
            os.system(cmd)
            sys.exit(1)
        except Exception as e:
            pprint("Error connecting to instance:" + str(e))
            sys.exit(1)

    def connect(self, instance_name, engine, database, user, secret):
        conninfo = self.find_in_dynamo(instance_name, engine)
        if not secret:
            secret = self.get_user_secret(user)
        try:
            cmd = "sudo docker run --rm -i -t {} sh -c \"psql postgresql://{}:{}@{}:{}/{}\"".format(
                self.get_docker_image(conninfo['EngineVersion']),
                user, urllib.parse.quote(secret), conninfo['Endpoint']['Address'], "5432", database)
            os.system(cmd)
            cmd = ""
            sys.exit(1)
        except Exception as e:
            pprint("Error connecting to instance:" + str(e))
            sys.exit(1)

    def add_instance_in_shelve(self, instance_name, engine):
        conndict = self.search_instances(instance_name, engine)
        key = str(self.country) + "-" + instance_name
        with shelve.open('/tmp/shelve_rds_conn') as d:
            if key in d.keys():
                idata = d[key]
        try:
            if "idata" in locals():
                if "inventory" in idata.keys():
                    conndict[0]['inventory'] = idata['inventory']
            self.save_data_in_shelve_key("/tmp/shelve_rds_conn", key, conndict[0])
        except Exception as e:
            pprint("Error adding instance to shelve:" + str(e))

    def add_instance_in_dynamo(self, instance_name, engine):
        conndict = self.search_instances(instance_name, engine)
        dynamordscli = DynamoRDSClient(self.country)
        key = dynamordscli.get_vars(instance_name)
        if key:
            idata = key['value']
        try:
            if "idata" in locals():
                if "inventory" in idata.keys():
                    conndict[0]['inventory'] = idata['inventory']
            dynamordscli.update_vars(instance_name, json.dumps(conndict[0], default=json_util.default))
        except Exception as e:
            pprint("Error adding instance to dynamo:" + str(e))

    def get_info(self, instance_name, engine, argument):
        try:
            conninfo = self.find_in_dynamo(instance_name, engine)
            aux = conninfo
            clean_aux = dict()
            if str(argument) != "all":
                splittedA = argument.split(";")
                for i in range(0, len(splittedA)):
                    if len(splittedA[i].split(".")) > 1:
                        splitted = splittedA[i].split(".")
                        clean_aux2 = aux[splitted[0]]
                        for j in range(1, len(splitted)):
                            clean_aux2 = clean_aux2[splitted[j]]
                        clean_aux[str(splittedA[i])] = clean_aux2
                    else:
                        clean_aux[splittedA[i]] = aux[splittedA[i]]
                aux = clean_aux
            print(json.dumps(aux, default=json_util.default, indent=4))
        except Exception as e:
            pprint("Error getting instance info:" + str(e))
            sys.exit(1)

    def get_info_shelve(self, instance_name, engine, argument):
        try:
            conninfo = self.find_in_shelve(instance_name, engine)
            aux = conninfo
            clean_aux = dict()
            if str(argument) != "all":
                splittedA = argument.split(";")
                for i in range(0, len(splittedA)):
                    if len(splittedA[i].split(".")) > 1:
                        splitted = splittedA[i].split(".")
                        clean_aux2 = aux[splitted[0]]
                        for j in range(1, len(splitted)):
                            clean_aux2 = clean_aux2[splitted[j]]
                        clean_aux[str(splittedA[i])] = clean_aux2
                    else:
                        clean_aux[splittedA[i]] = aux[splittedA[i]]
                aux = clean_aux
            print(json.dumps(aux, default=json_util.default, indent=4))
        except Exception as e:
            pprint("Error getting instance info:" + str(e))
            sys.exit(1)

    def arrange_inventory(self, engine):
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]

        with open(git_root + '/.rds_postgresql_endpoints', 'r') as file:
            line_list = file.read().splitlines()[1:]
            for line in line_list:
                store_line = self.generate_inv_format(line)
                if store_line:
                    self.add_instance_inventory(store_line)

    def arrange_dynamo(self, engine):
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]

        with open(git_root + '/.rds_postgresql_endpoints', 'r') as file:
            line_list = file.read().splitlines()[1:]
            for line in line_list:
                store_line = self.generate_inv_format(line)
                if store_line:
                    self.add_instance_inventory_dynamo(store_line)

    def return_correct_country(self, profile, instance):
        tags = self.checkInstanceTags(instance)
        if "COUNTRY" in tags.keys():
            if str(tags['COUNTRY']).lower() not in s.COUNTRIES:
                if profile == "xx1-xx-east-1":
                    country = "m"
                elif profile == "xx-xx-east-1":
                    country = "a"
                elif profile == "xxx-xxx-east-1":
                    country = "dev"
                elif profile == "xxxx-xx-west-2":
                    country = "xx1"
                elif profile == "xx-xx-west-2":
                    country = "c"
                else:
                    country = self.country.lower()
            else:
                country = str(tags['COUNTRY']).lower()
        else:
            if profile == "xx1-xx-east-1":
                country = "m"
            elif profile == "xx-xx-east-1":
                country = "a"
            elif profile == "xxx-xxx-east-1":
                country = "dev"
            elif profile == "xxxx-xx-west-2":
                country = "xx1"
            elif profile == "xx-xx-west-2":
                country = "c"
            else:
                country = self.country.lower()
        return country

    def generate_new_csv(self):
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
        try:
            items_rds = dict()
            for country in s.COUNTRIES:
                new_list = []
                dynamordscli = DynamoRDSClient(country)
                list_of_dict = dynamordscli.get_all_ids()["Items"]
                for eachdict in list_of_dict:
                    new_list.append(eachdict["id"])
                items_rds[country] = new_list
            for profile in s.AWS_PROFILES_UNIQUE:
                self.setSessionByProfile(profile)
                filter_list = []
                filter_list.append({'Name': 'engine', 'Values': ['postgres', 'mysql', 'docdb', 'aurora-mysql', 'aurora', 'aurora-postgresql']})
                pages = self.search_all_instances(filter_list)
                for page in pages:
                    for instance in page["DBInstances"]:
                        country = self.return_correct_country(profile, instance)
                        if instance["DBInstanceIdentifier"] not in items_rds[country]:
                            pprint("Adding " + instance["DBInstanceIdentifier"])
                            self.setSession(country)
                            instance_inv_data = self.generate_inv_data(instance, country)
                            idata = self.find_in_dynamo(instance["DBInstanceIdentifier"], instance["Engine"])
                            if "inventory" not in idata.keys():
                                self.add_instance_inventory_dynamo(instance_inv_data)
                            elif idata["inventory"]["instance_role"] != instance_inv_data["instance_role"]:
                                dynamordscli = DynamoRDSClient(self.country)
                                idata["inventory"]["instance_role"] = instance_inv_data["instance_role"]
                                dynamordscli.update_vars(idata["DBInstanceIdentifier"], idata)

            if not os.path.isfile(git_root + "/.rds_postgresql_endpoints"):
                shutil.copy2("~/.rds_postgresql_endpoints", git_root + "/.rds_postgresql_endpoints")
            shutil.move(git_root + "/.rds_postgresql_endpoints", git_root + "/.rds_postgresql_endpoints.bkp")
            open(git_root + "/.rds_postgresql_endpoints", 'a').close()
            for country in s.COUNTRIES:
                dynamordscli = DynamoRDSClient(country)
                page = dynamordscli.get_all()
                for instance in page['Items']:
                    instance_name = instance['id']
                    instance_data = json.loads(instance['value'])
                    if instance_data['Engine'] == "postgres":
                        if "inventory" in instance_data.keys():
                            with open(git_root + '/.rds_postgresql_endpoints', 'a') as file:
                                file.writelines('{0}:{1}:{2}:{3}:{4}:{5}:{6}:{7}:{8}{9}'
                                                .format(instance_data['inventory']['country'],
                                                        instance_data['inventory']['instance_module'],
                                                        instance_data['inventory']['instance_alias'],
                                                        instance_data['inventory']['instance_name'],
                                                        instance_data['inventory']['instance_endpoint'],
                                                        instance_data['inventory']['instance_cname'],
                                                        instance_data['inventory']['instance_port'],
                                                        instance_data['inventory']['instance_tunnel_port'],
                                                        instance_data['inventory']['instance_role'],
                                                        '\n'))
        except Exception as e:
            pprint("Error generating new inventory csv:" + str(e))
            pprint(instance)
            sys.exit(1)

    def generate_new_csv_shelve(self):
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
        try:
            for profile in s.AWS_PROFILES_UNIQUE:
                self.setSessionByProfile(profile)
                filter_list = []
                filter_list.append({'Name': 'engine', 'Values': ['postgres']})
                pages = self.search_all_instances(filter_list)
                for page in pages:
                    for instance in page["DBInstances"]:
                        country = self.return_correct_country(profile, instance)
                        self.setSession(country)

                        instance_inv_data = self.generate_inv_data(instance, country)
                        idata = self.find_in_shelve(instance["DBInstanceIdentifier"], instance["Engine"])
                        if "inventory" not in idata.keys():
                            self.add_instance_inventory(instance_inv_data)
                        elif idata["inventory"]["instance_role"] != instance_inv_data["instance_role"]:
                            d = shelve.open("/tmp/shelve_rds_conn")
                            d[country + "-" + idata["DBInstanceIdentifier"]]["inventory"]["instance_role"] = \
                                instance_inv_data["instance_role"]
                            d.close()

            d = shelve.open("/tmp/shelve_rds_conn", writeback=True)
            with open(git_root + '/.rds_postgresql_endpoints', 'w') as file:
                for key in d.keys():
                    if "inventory" in d[key].keys():
                        file.writelines('{0}:{1}:{2}:{3}:{4}:{5}:{6}:{7}:{8}{9}'
                                        .format(d[key]['inventory']['country'],
                                                d[key]['inventory']['instance_module'],
                                                d[key]['inventory']['instance_alias'],
                                                d[key]['inventory']['instance_name'],
                                                d[key]['inventory']['instance_endpoint'],
                                                d[key]['inventory']['instance_cname'],
                                                d[key]['inventory']['instance_port'],
                                                d[key]['inventory']['instance_tunnel_port'],
                                                d[key]['inventory']['instance_role'],
                                                '\n'))
            d.close()
        except Exception as e:
            pprint("Error generating new inventory csv:" + str(e))
            sys.exit(1)

    def formatAwsTags(self, aws_instance_tags):
        """
        Genera el array de tags
        """
        try:
            formatted_list = dict()
            for each_tag in aws_instance_tags:
                name = str(each_tag['Key']).upper()
                value = str(each_tag['Value']).upper()
                formatted_list[name] = value
            return formatted_list

        except Exception as e:
            pprint("Error formatting instance tags:" + str(e))
            sys.exit(1)

    def checkInstanceTags(self, instance_data, throttle=0.2):
        """
        Chequeo que los tags coincidan con los propios de la instancia
        """
        try:

            if "Taglist" not in instance_data.keys():
                instance_arn = instance_data["DBInstanceArn"]
                aws_instance_tags = self.infra_client.list_tags_for_resource(
                    ResourceName=instance_arn
                )
            else:
                aws_instance_tags = instance_data
        except Exception as e:
                if "Throttling" in str(e):
                    time.sleep(throttle)
                    return self.checkInstanceTags(instance_data, throttle * 2)
                else:
                    pprint("Error getting aws instance tags:" + str(e))
                    sys.exit(1)
        else:
            returned_tags = aws_instance_tags['TagList']
            returned_tags = self.formatAwsTags(returned_tags)
            return returned_tags

    def setSession(self, country):
        if country == "tool":
            country = "c"
        self.country = country
        self.region = s.REGION[self.country]
        self.session = boto3.session.Session(profile_name=s.AWS_PROFILE[self.country])
        self.infra_client = self.session.client('rds', region_name=self.region)

    def setSessionByProfile(self, aws_profile):
        country = aws_profile.split('-', 1)[0]
        region = aws_profile.split('-', 1)[1]
        if country == "tool":
            country = "c"
        self.country = country
        self.region = region
        self.session = boto3.session.Session(profile_name=aws_profile)
        self.infra_client = self.session.client('rds', region_name=region)

    def generate_inv_format(self, line_inventory, engine="postgres"):
        line_array = line_inventory.split(":")
        store_line = dict()
        store_line['country'] = str(line_array[0]).lower()
        store_line['instance_module'] = str(line_array[1]).lower()
        store_line['instance_alias'] = str(line_array[2]).lower()
        store_line['instance_name'] = str(line_array[3]).lower()
        self.setSession(str(store_line['country']).lower())
        idata = self.find_in_dynamo(store_line['instance_name'], engine)
        store_line['instance_endpoint'] = str(line_array[4]).lower()
        store_line['instance_cname'] = str(line_array[5]).lower()
        cname_to_endpoint = os.popen(
            "nslookup " + store_line['instance_cname'] + " | grep Name | cut -d: -f2 | xargs").readline().strip()
        if str(cname_to_endpoint) != str(idata['Endpoint']['Address']):
            pprint(
                "Error en cname: " + store_line['instance_cname'] + "; endpoint: " + idata['Endpoint']['Address'])
            raise Exception("Instance CNAME does not relates to saved endpoint")
        store_line['instance_port'] = str(line_array[6]).lower()
        store_line['instance_tunnel_port'] = str(line_array[7]).lower()
        store_line['instance_role'] = str(line_array[8]).lower()
        return store_line

    def simple_inv_format(self, line_inventory):
        line_array = line_inventory.split(":")
        store_line = dict()
        store_line['country'] = str(line_array[0]).lower()
        store_line['instance_module'] = str(line_array[1]).lower()
        store_line['instance_alias'] = str(line_array[2]).lower()
        store_line['instance_name'] = str(line_array[3]).lower()
        store_line['instance_endpoint'] = str(line_array[4]).lower()
        store_line['instance_cname'] = str(line_array[5]).lower()
        store_line['instance_port'] = str(line_array[6]).lower()
        store_line['instance_tunnel_port'] = str(line_array[7]).lower()
        store_line['instance_role'] = str(line_array[8]).lower()
        return store_line

    def generate_inv_data(self, instance_data, country):
        store_line = dict()
        store_line['country'] = country
        store_line['instance_module'] = str(instance_data["DBInstanceIdentifier"]).lower()
        store_line['instance_alias'] = str(instance_data["DBInstanceIdentifier"]).lower()
        store_line['instance_name'] = str(instance_data["DBInstanceIdentifier"]).lower()
        store_line['instance_endpoint'] = str(instance_data['Endpoint']['Address']).lower()
        store_line['instance_cname'] = store_line['instance_endpoint']
        store_line['instance_port'] = instance_data['Endpoint']['Port']
        store_line['instance_tunnel_port'] = instance_data['Endpoint']['Port']
        if "ReadReplicaSourceDBInstanceIdentifier" in instance_data.keys():
            store_line['instance_role'] = 'replica'
        else:
            store_line['instance_role'] = 'master'
        return store_line

    def add_instance_inventory(self, instance_inv_data):
        try:
            d = shelve.open("/tmp/shelve_rds_conn")
            key = str(instance_inv_data['country']).lower() + "-" + instance_inv_data['instance_name']
            if key in d.keys():
                idata = d[key]
                d.close()
                if "inventory" not in idata.keys():
                    idata['inventory'] = instance_inv_data
                    self.save_data_in_shelve_key("/tmp/shelve_rds_conn", key, idata)
            else:
                d.close()

        except Exception as e:
            pprint("Error adding static inventory data to shelve:" + str(e))
            sys.exit(1)

    def add_instance_inventory_dynamo(self, instance_inv_data):
            key = str(instance_inv_data["instance_name"])
            dynamordscli = DynamoRDSClient(instance_inv_data["country"])
            item = dynamordscli.get_vars(key)
            if item:
                values = json.loads(item['value'])
                values['last_check'] = time.time()
                if "inventory" not in values.keys():
                    values['inventory'] = instance_inv_data
                    dynamordscli.update_vars(key, json.dumps(values, default=json_util.default))
                else:
                    dynamordscli.update_vars(key, json.dumps(values, default=json_util.default))
            else:
                idata = self.search_instances(key)[0]
                idata['inventory'] = instance_inv_data
                idata['last_check'] = time.time()
                dynamordscli.update_vars(key, json.dumps(idata, default=json_util.default))


    def delete_instance_from_shelve(self, instance_name):
        try:
            d = shelve.open("/tmp/shelve_rds_conn")
            key = str(self.country).lower() + "-" + instance_name
            if key in d.keys():
                del d[key]
            else:
                pprint("La instancia no existe en shelve")
            d.close()

        except Exception as e:
            pprint("Error deleting instance data from shelve:" + str(e))
            sys.exit(1)

    def delete_instance_from_dynamo(self, instance_name):
        try:
            dynamordscli = DynamoRDSClient(self.country)
            data=dynamordscli.get_vars(str(instance_name))
            if not data:
                raise Exception("Instance does not exist")
            dynamordscli.delete_vars(str(instance_name))
        except Exception as e:
            pprint("Error deleting instance {} in country {} data from dynamo: {}".format(instance_name,
                                                                                          self.country, str(e)))

    def run_shelve(self, instance_name, engine, database, user, secret, file, query):
        conninfo = self.find_in_shelve(instance_name, engine)
        source = conninfo['Endpoint']['Address']
        port = conninfo['Endpoint']['Port']

        if not secret:
            secret = self.get_user_secret(user)
        try:
            conn = self.openConn(database, user, secret, port, source)
            conn.autocommit = True
            cur = conn.cursor()
            if query:
                cur.execute(query)
            elif file:
                if os.path.isfile(str(os.path.realpath(__file__)) + file):
                    cur.execute(open(str(os.path.realpath(__file__)) + file, "r").read())
                elif os.path.isfile(str(file)):
                    cur.execute(open(str(file), "r").read())
                else:
                    raise Exception("El archivo no existe")
            else:
                raise Exception("No hay nada que ejecutar")

            result = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            print(column_names)
            print(result)
            cur.close()
            conn.close()

        except Exception as e:
            pprint("Error executing query:" + str(e))
            if "cur" in locals():
                cur.close()
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def run(self, instance_name, engine, database, user, secret, file, query):
        conninfo = self.find_in_dynamo(instance_name, engine)
        source = conninfo['Endpoint']['Address']
        port = conninfo['Endpoint']['Port']

        if not secret:
            secret = self.get_user_secret(user)
        try:
            conn = self.openConn(database, user, secret, port, source)
            conn.autocommit = True
            cur = conn.cursor()
            if query:
                cur.execute(query)
            elif file:
                if os.path.isfile(str(os.path.realpath(__file__)) + file):
                    cur.execute(open(str(os.path.realpath(__file__)) + file, "r").read())
                elif os.path.isfile(str(file)):
                    cur.execute(open(str(file), "r").read())
                else:
                    raise Exception("El archivo no existe")
            else:
                raise Exception("No hay nada que ejecutar")

            result = cur.fetchall()
            column_names = list([desc[0] for desc in cur.description])
            print(tabulate(result, headers=[str(elem) for elem in column_names], tablefmt='psql'))
            cur.close()
            conn.close()

        except Exception as e:
            if "no results to fetch" in str(e):
                pprint("Query ejecutada. No hay resultados que mostrar.")
            else:
                pprint("Error executing query:" + str(e))
                pprint(traceback.format_exc())
            if "cur" in locals():
                cur.close()
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def openConn(self, database, user, secret, port, source):
        """
        Apertura de connector
        """
        # pprint("openConn")
        try:
            return psycopg2.connect(
                dbname=database, user=user, port=port, host=source, password=secret)

        except Exception as e:
            pprint("Error connecting to endpoint:" + str(e))
            sys.exit(1)

    def modify_inventory_shelve(self, instance, parameter):
        d = shelve.open("/tmp/shelve_rds_conn")
        key = str(self.country).lower() + "-" + instance
        if key in d.keys():
            param = str(parameter).split("=")
            if str(param[0]) in d[key]["inventory"].keys():
                d[key]["inventory"][str(param[0])] = str(param[1])
                print(json.dumps(d[key]["inventory"], default=json_util.default, indent=4))
        else:
            pprint("La instancia no existe en shelve")
        d.close()

    def modify_csv_line(self, idata):
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
        with open(git_root + '/.rds_postgresql_endpoints_new', 'w') as newfile:
            file = open(git_root + '/.rds_postgresql_endpoints', 'r')
            line_list = file.read().splitlines()[1:]
            for line in line_list:
                store_line =  self.simple_inv_format(line)
                if store_line['country'] == idata['inventory']['country'] and \
                        store_line['instance_name'] == idata['inventory']['instance_name']:
                    newfile.writelines('{0}:{1}:{2}:{3}:{4}:{5}:{6}:{7}:{8}{9}'
                                    .format(idata['inventory']['country'],
                                            idata['inventory']['instance_module'],
                                            idata['inventory']['instance_alias'],
                                            idata['inventory']['instance_name'],
                                            idata['inventory']['instance_endpoint'],
                                            idata['inventory']['instance_cname'],
                                            idata['inventory']['instance_port'],
                                            idata['inventory']['instance_tunnel_port'],
                                            idata['inventory']['instance_role'],
                                            '\n'))
                else:
                    newfile.writelines(line+'\n')
            file.close()
        shutil.copy2(git_root + "/.rds_postgresql_endpoints_new", git_root + "/.rds_postgresql_endpoints")
        if os.path.isfile("~/.rds_postgresql_endpoints"):
            shutil.copy2(git_root + "/.rds_postgresql_endpoints", "~/.rds_postgresql_endpoints")

    def modify_inventory(self, instance, parameter):
        idata = self.find_in_dynamo(instance, "")
        if idata:
            param = str(parameter).lower().split("=")
            if str(param[0]) in idata["inventory"].keys():
                idata["inventory"][str(param[0])] = str(param[1])
                self.save_data_in_dynamo_key(instance, idata)
                print(json.dumps(idata["inventory"], default=json_util.default, indent=4))
                self.modify_csv_line(idata)
        else:
            pprint("La instancia no existe en dynamo")

    def populate_all_rds_dynamo(self):
        git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
        try:
            items_rds = dict()
            for country in s.COUNTRIES:
                new_list = []
                dynamordscli = DynamoRDSClient(country)
                list_of_dict = dynamordscli.get_all_ids()["Items"]
                for eachdict in list_of_dict:
                    new_list.append(eachdict["id"])
                items_rds[country] = new_list
            for profile in s.AWS_PROFILES_UNIQUE:
                self.setSessionByProfile(profile)
                pages = self.search_all_instances()
                for page in pages:
                    for instance in page["DBInstances"]:
                        country = self.return_correct_country(profile, instance)
                        if instance["DBInstanceIdentifier"] not in items_rds[country]:
                            pprint("Adding " + instance["DBInstanceIdentifier"])
                            self.setSession(country)
                            instance_inv_data = self.generate_inv_data(instance, country)
                            idata = self.find_in_dynamo(instance["DBInstanceIdentifier"], instance["Engine"])
                            if "inventory" not in idata.keys():
                                self.add_instance_inventory_dynamo(instance_inv_data)
                            elif idata["inventory"]["instance_role"] != instance_inv_data["instance_role"]:
                                dynamordscli = DynamoRDSClient(self.country, profile)
                                idata["inventory"]["instance_role"] = instance_inv_data["instance_role"]
                                dynamordscli.update_vars(idata["DBInstanceIdentifier"], idata)

        except Exception as e:
            pprint("Error populating dynamo:" + str(e))
            sys.exit(1)




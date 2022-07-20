import boto3
from .settings import Settings as s
from bson import json_util
import json
from botocore.exceptions import ClientError
from inspect import getmembers
from pprint import pprint


class DocDBClient:

    def __init__(self, country):
        self.country = country
        self.region = s.REGION[self.country]
        self.session = boto3.session.Session(profile_name=s.AWS_PROFILE[self.country])
        self.infra_client = self.session.client('docdb', region_name=self.region)

    def get_rds_instance_description(self, docdb_instance_id):
        return self.infra_client.describe_db_instances(
            InstanceIds=[
                docdb_instance_id
            ]
        )

    def get_rds_all_instances(self):
        return self.infra_client.describe_db_instances()

    def search_instances(self, instance_name):
        result = self.infra_client.describe_db_instances(DBInstanceIdentifier=instance_name)
        return result["DBInstances"]

    def search_clusters(self, instance_name):
        result = self.infra_client.describe_db_clusters(DBClusterIdentifier=instance_name)
        return result["DBClusters"]

    def __eqprof__(self, other):
        if not isinstance(other, DocDBClient):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return s.AWS_PROFILE[self.country] == s.AWS_PROFILE[other.country]

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

    def search_all_clusters(self, var_filter="", throttle=0.2):
        """
      Get list of all the DBs from RDS
      """
        try:
            paginator = self.infra_client.get_paginator('describe_db_clusters')
            if var_filter:
                return paginator.paginate(Filters=var_filter)
            return paginator.paginate()
        except Exception as e:
            if "Throttling" in str(e):
                time.sleep(throttle)
                return self.search_all_clusters(var_filter, throttle * 2)
            else:
                pprint("Error searching all clusters:" + str(e))
                sys.exit(1)

    def setSessionByProfile(self, aws_profile):
        country = aws_profile.split('-', 1)[0]
        region = aws_profile.split('-', 1)[1]
        if country == "tool":
            country = "c"
        self.country = country
        self.region = region
        self.session = boto3.session.Session(profile_name=aws_profile)
        self.infra_client = self.session.client('rds', region_name=region)

    def generate_inv_data(self, instance_data, country):
        store_line = dict()
        store_line['country'] = country
        store_line['instance_module'] = str(instance_data["DBInstanceIdentifier"]).lower()
        store_line['instance_alias'] = str(instance_data["DBInstanceIdentifier"]).lower()
        store_line['instance_id'] = str(instance_data["DBInstanceIdentifier"]).lower()
        store_line['instance_endpoint'] = str(instance_data['Endpoint']['Address']).lower()
        store_line['instance_cname'] = store_line['instance_endpoint']
        store_line['instance_port'] = instance_data['Endpoint']['Port']
        store_line['instance_tunnel_port'] = instance_data['Endpoint']['Port']
        store_line['instance_role'] = 'master'
        return store_line

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
                filter_list.append({'Name': 'engine', 'Values': ['docdb']})
                pages = self.search_all_clusters(filter_list)
                for page in pages:
                    for instance in page["DBClusters"]:
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
            sys.exit(1)

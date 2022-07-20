#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Create ansible global inventory for RDS instances
import json
import os
import subprocess
import pprint
import yaml
import sys
import argparse

git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import rds
from lib import ec2
from libraries.format_engine import FormatService
from lib.settings import Settings as s

global_inventory_profile = "global_inventory_profile.yaml"
global_inventory_country = "global_inventory_country.yaml"
# os.system('clear')

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

parser = argparse.ArgumentParser(description='Checking Table-Databases Showing Custom Queries Results')

parser.add_argument('--profile', action="store", dest="profile", help="xx-xx-east-1 | "
                                                                                              "xx-xx-west-2 | "
                                                                                              "xx1-xx-east-1 | "
                                                                                              "xx1-xx-west-2 | "
                                                                                              "xxx-xxx-east-1 | "
                                                                                              "xxx-xxx-west-2 | "
                                                                                              "xx1-xx-east-1 | "
                                                                                              "xxxx-xx-west-2",
                    required=True, type=valid_profile)
args = parser.parse_args()

fullprofile = None
if args.profile:
    fullprofile = args.profile


def executeCommand(cmd):
    try:
        f = open(global_inventory, "w")
        result = subprocess.run([cmd], check=True, timeout=300, shell=True, stdout=f)
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        f.close()

def callDynamicInventory():
    """
  Use the ansible dynamic inventory plugin
  """
    AnsibleCmd = "ansible-inventory -i " + global_inventory + " --export --yaml --list"
    executeCommand(AnsibleCmd)


def put_inventory_engine_host(inventory: dict, engine: str, instance_data: dict) -> dict:
    if "hosts" in inventory["all"]["children"][engine].keys():
        inventory["all"]["children"][engine]["hosts"].update({
            instance_data["name"]: {"anansiblevar_host": instance_data["host"], "anansiblevar_port": instance_data["port"]}})
    else:
        inventory["all"]["children"][engine]["hosts"] = {
            instance_data["name"]: {"anansiblevar_host": instance_data["host"], "anansiblevar_port": instance_data["port"]}}
    return inventory


def put_inventory_engine(inventory: dict, engine: str) -> dict:
    if engine not in inventory["all"]["children"].keys():
        inventory["all"]["children"].update({engine: {}})
    return inventory


def put_inventory_country(inventory: dict, country: str, engine: str) -> dict:
    if country not in inventory["all"]["children"].keys():
        inventory["all"]["children"].update({country: {"children": {}}})

    if engine not in inventory["all"]["children"][country]["children"].keys():
        inventory["all"]["children"][country]["children"].update({engine: {}})
    return inventory


def put_inventory_country_host(inventory: dict, country: str, engine: str, instance_data: dict) -> dict:
    if "hosts" in inventory["all"]["children"][country]["children"][engine].keys():
        inventory["all"]["children"][country]["children"][engine]["hosts"].update({
            instance_data["name"]: {"anansiblevar_host": instance_data["host"], "anansiblevar_port": instance_data["port"]}})
    else:
        inventory["all"]["children"][country]["children"][engine]["hosts"] = {
            instance_data["name"]: {"anansiblevar_host": instance_data["host"], "anansiblevar_port": instance_data["port"]}}
    return inventory


def save_inventory(key: str, inv_dict: dict):
    if not os.path.isdir("{}/ansible/group_vars".format(git_root)):
        os.mkdir("{}/ansible/group_vars".format(git_root))
    if not os.path.isdir("{}/ansible/group_vars/{}".format(git_root, key)):
        os.mkdir("{}/ansible/group_vars/{}".format(git_root, key))
    with open("{}/ansible/group_vars/{}/{}".format(git_root, key, "dynamic_inventory.yaml"), 'w') as outfile:
        yaml.dump(inv_dict, outfile, default_flow_style=False)


def make_global_inventory():
    inventory_country = {"all": {"children": {}}}

    if not fullprofile:
        list_prof = s.AWS_PROFILES_UNIQUE
    else:
        list_prof = [fullprofile]

    for profile in list_prof:
        inventory_yaml = {"all": {"children": {}}}
        rds_cli = rds.RDSClient('c')
        rds_cli.setSessionByProfile(profile)
        pages = rds_cli.search_all_instances()
        for page in pages:
            for instance in page["DBInstances"]:
                service = FormatService('rds')
                instance_data = service.format_important_data(instance)
                engine = instance["Engine"].replace("-", "_")
                inventory_yaml = put_inventory_engine(inventory_yaml, engine)
                inventory_yaml = put_inventory_engine_host(inventory_yaml, engine, instance_data)
                country = rds_cli.return_correct_country(profile, instance)
                inventory_country = put_inventory_country(inventory_country, country, engine)
                inventory_country = put_inventory_country_host(inventory_country, country, engine, instance_data)

        #ec2_cli = ec2.EC2Client('c')
        #ec2_cli.setSessionByProfile(profile)
        #instances = ec2_cli.search_instances("mongo")
        #for instance in instances:
        #    service = FormatService('ec2')
        #    instance_data = service.format_important_data(instance)
        #    pprint(instance_data)
        save_inventory(profile, inventory_yaml)

    for country in inventory_country["all"]["children"].keys():
        new_dict = {"all": {}}
        new_dict["all"].update(inventory_country["all"]["children"][country])
        save_inventory(country, new_dict)

def make_global_inventory_audit():
    inventory_payments_postgres_yaml = {"payments": {"hosts": {}}}
    inventory_payments_docdb_yaml = {"payments": {"hosts": {}}}
    if not fullprofile:
        list_prof = s.AWS_PROFILES_UNIQUE
    else:
        list_prof = [fullprofile]

    for profile in list_prof:
        rds_cli = rds.RDSClient('c')
        rds_cli.setSessionByProfile(profile)
        pages = rds_cli.search_all_instances()
        for page in pages:
            for instance in page["DBInstances"]:
                service = FormatService('rds')
                instance_data = service.format_important_data(instance)
                country = rds_cli.return_correct_country(profile, instance)

                if 'Engine' in instance and instance["Engine"] == "postgres":
                    if 'TagList' in instance:
                        for tag in instance['TagList']:
                            if 'Payments' in tag.values():
                                inventory_payments_postgres_yaml['payments']['hosts'].update({instance_data["name"]: {"anansiblevar_host": instance_data["host"], "anansiblevar_port": instance_data["port"], "anansiblevar_country": country.upper(), "anansiblevar_s3": "auditory-logs"}})

                if 'Engine' in instance and instance["Engine"] == "docdb":
                    if 'TagList' in instance:
                        for tag in instance['TagList']:
                            if 'Payments' in tag.values():
                                inventory_payments_docdb_yaml['payments']['hosts'].update({instance_data["name"]: {"anansiblevar_host": instance_data["host"], "anansiblevar_port": instance_data["port"], "anansiblevar_country": country.upper(), "anansiblevar_groupname": instance['DBClusterIdentifier'], "anansiblevar_s3": "auditory-logs"}})

    with open("{}/ansible/group_vars/{}".format(git_root, "inventory_payments_postgres.yaml"), 'w') as outfile:
        yaml.dump(inventory_payments_postgres_yaml, outfile, default_flow_style=False)

    with open("{}/ansible/group_vars/{}".format(git_root, "inventory_test.yaml"), 'w') as outfile:
        yaml.dump(inventory_payments_docdb_yaml, outfile, default_flow_style=False)
def main():
    make_global_inventory()
    make_global_inventory_audit()

if __name__ == '__main__':
    main()

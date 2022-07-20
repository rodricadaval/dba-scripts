#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform auto-scaling to docdb replicas on nightshifts.
# Collect vital stats when needed

import os
import boto3
from pprint import pprint
import sys
import argparse
import shelve
import time

#os.system('clear')

parser = argparse.ArgumentParser(description='Downscale DocDB Replica instances')

parser.add_argument('-a', '--account', action="store", dest="awsaccount", default="xx",
                    help="AWS Account: xx, xxx, x1, x2, x3", required=True)
parser.add_argument('-r', '--region', action="store", dest="awsregion", default="xx-west-2",
                    help="Name of the aws credential profile", required=True)
parser.add_argument('-e', '--engine', action="store", dest="dbengine", default="docdb", help="rds, mysql or docdb")
parser.add_argument('--action', action="store", dest="action", help="check, process, get-by-tag")
parser.add_argument('--tags', action="store", dest="tags", help="name=value;name2=value2")


args = parser.parse_args()

awsaccount = args.awsaccount
awsregion = args.awsregion
rdsengine = args.dbengine
if args.action == "get-by-tag":
    if args.tags:
        tags = args.tags

if str(rdsengine) == "docdb":
    infra = "docdb"
else:
    infra = "rds"

fullprofile = awsaccount + '-' + awsregion
#pprint("Region: " + fullprofile)
boto3.setup_default_session(profile_name=fullprofile)
d = shelve.open("/tmp/shelve_tags_inventory_" + awsaccount + "_" + awsregion + "_" + rdsengine, writeback=True)

def saveInShelve(instance, tags):
    d[str(instance)] = tags

def compareTags(aws_instance_tags):
    """
    Compara los tags enviados por parametro con los propios de la instancia. Devuelve True or False
    """
    array_tags = tags.split(";")
    for each_tag in array_tags:
        name = str(each_tag).split("=")[0].upper()
        value = str(each_tag).split("=")[1].upper()
        list_val = []
        values = value.split(",")
        for each_val in values:
            list_val.append(each_val)

        if name in aws_instance_tags.keys():
            if aws_instance_tags[name] not in list_val:
                return False
        else:
            return False

    return True


def formatAwsTags(aws_instance_tags):
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


def listInstacesByTag():
    list = []
    for each_instance in d.keys():
        if compareTags(d[str(each_instance)]):
            list.append(each_instance)
    return list


def checkTags(data):
    """
    Busco tags de la instancia
    """

    if str(rdsengine) == "docdb":
        arn = data["DBClusterArn"]
    else:
        arn = data["DBInstanceArn"]

    try:
        rdsclient = boto3.client(infra)
        aws_instance_tags = rdsclient.list_tags_for_resource(
            ResourceName=arn
        )

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(0.4)
            return checkTags(data)
        else:
            pprint("Error getting aws instance data:" + str(e))
            sys.exit(1)

    else:
        return formatAwsTags(aws_instance_tags['TagList'])


def processInstances(instances):
    """

    Recorro las instancias.
    Luego, guardo arn.
    """

    if rdsengine == "docdb":
        param = "DBClusters"
    else:
        param = "DBInstances"
    for i in instances[str(param)]:
        if rdsengine == "docdb":
            instance = i["DBClusterIdentifier"]
        else:
            instance = i["DBInstanceIdentifier"]
        tags = checkTags(i)
        saveInShelve(instance, tags)


def gatherRdsInfo():
    """
  Get list of all the DBs from RDS
  """
    pprint("Using Shelve: shelve_tags_inventory_" + awsaccount + "_" + awsregion + "_" + rdsengine)

    rdsclient = boto3.client(infra)
    if str(rdsengine) == "docdb":
        paginator = rdsclient.get_paginator('describe_db_clusters')
    else:
        paginator = rdsclient.get_paginator('describe_db_instances')
    filter_list = []
    filter_list.append({'Name': 'engine', 'Values': [rdsengine]})
    pages = paginator.paginate(Filters=filter_list)

    for page in pages:
        processInstances(page)


def main():
    if args.action == "check":
        dkeys = list(d.keys())
        for x in dkeys:
            print(x, d[x])
    elif args.action == "process":
        gatherRdsInfo()
    elif args.action == "get-by-tag":
        print(listInstacesByTag())
    else:
        print("There are no actions with this name")
    d.close()

    #print()
    #pprint("Finished")


if __name__ == '__main__':
    main()

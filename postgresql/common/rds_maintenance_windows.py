#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@acompanydomain.com
#
# Monitor anomality and active threads/process.
# Collect vital stats when needed

import os
import requests
import json
from base64 import b64encode
import boto3
import datetime
import subprocess
import psycopg2
import re
from pprint import pprint
import sys
import argparse
import shelve
import time
import shutil

os.system('clear')

parser = argparse.ArgumentParser(description='Check RDS maintenance windows')

parser.add_argument('-a', '--account', action="store", dest="awsaccount", default="xx",
                    help="AWS Account: xx, xxx, x1, x2, x3")
parser.add_argument('-p', '--region', action="store", dest="awsregion", default="xx-west-2",
                    help="Name of the aws credential profile")
parser.add_argument('-e', '--engine', action="store", dest="dbengine", default="postgres", help="postgres, mysql or docdb")

args = parser.parse_args()

awsaccount = args.awsaccount
awsregion = args.awsregion
rdsengine = args.dbengine
global fullData
fullData = {}
fullprofile = awsaccount + '-' + awsregion
pprint("Region: " + fullprofile)
# pprint(idata)
boto3.setup_default_session(profile_name=fullprofile)

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


def processInstances(rdsInstances):
    """

    Recorro las instancias en búsqueda de replicas.
    Luego, guardo datos actuales.
    """

    for i in rdsInstances["DBInstances"]:
        fullData[i["DBInstanceIdentifier"]] = i["PreferredMaintenanceWindow"]


def getRDSMaintenance(rdsengine):
    """
  Get list of all the DBs from RDS
  """

    rdsclient = boto3.client('rds')
    paginator = rdsclient.get_paginator('describe_db_instances')
    pages = paginator.paginate(Filters=[{'Name': 'engine', 'Values': [rdsengine]}])

    for page in pages:
        processInstances(page)

    return pprint(fullData)


def main():
    getRDSMaintenance(rdsengine)


if __name__ == '__main__':
    main()

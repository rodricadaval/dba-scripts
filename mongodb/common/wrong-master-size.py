#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Perform auto-scaling to docdb replicas on nightshifts.
# Collect vital stats when needed

import os
import requests
import json
from base64 import b64encode
import boto3
import datetime
import subprocess
import re
from pprint import pprint
import sys
import argparse
import shelve
import time
import shutil
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import opsgenie
from lib import dynamo
import logging
import getpass
import traceback

os.system('clear')

parser = argparse.ArgumentParser(description='Downscale DocDB Replica instances')

parser.add_argument('-a', '--account', action="store", dest="awsaccount", default="xx",
                    help="AWS Account: xx, xxx, x1, x2, x3", required=True)
parser.add_argument('-r', '--region', action="store", dest="awsregion", default="xx-west-2",
                    help="Name of the aws credential profile", required=True)

args = parser.parse_args()

awsaccount = args.awsaccount
awsregion = args.awsregion
rdsengine = 'docdb'

fullprofile = awsaccount + '-' + awsregion
pprint("Region: " + fullprofile)
boto3.setup_default_session(profile_name=fullprofile)

slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXX"
slackChannel = "dba_boto3_reports"

sizes_ordered = ["small", "medium", "large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge",
		 "18xlarge", "24xlarge"]


def sendToSlack(replica, master):
    """
    Slack
    """
    color = "#FF164f"
    thumb = "https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", "pretext": "Size invertido para ' + \
                   master["instance"] + '", \
            "author_name": "Master Instance Wrong Size", \
            "fields": [ \
            {"title": "Actual Master Name", "value": "' + str(master["instance"]) + '", "short": true}, \
            {"title": "Actual Master Class", "value": "' + str(master["size"]) + '","short": true}, \
            {"title": "Bigger Replica Name","value": "' + str(replica["instance"]) + '","short": true}, \
            {"title": "Bigger Replica Class","value": "' + str(replica["size"]) + '","short": true}, \
            {"title": "Region","value": "' + str(awsregion) + '","short": true}, \
            {"title": "AWS Account","value": "' + str(awsaccount) + '","short": true}, \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "last check", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-DocDBWrongMasterBot", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


def bigger(replica_size, master_size):
    return sizes_ordered.index(master_size.split(".")[2]) < sizes_ordered.index(replica_size.split(".")[2])


def get_instance_size(instance, throttle=0.2):
    try:
        rdsclient = boto3.client('docdb')
        awsInstanceData = rdsclient.describe_db_instances(
            DBInstanceIdentifier=instance,
            Filters=[
                {
                    'Name': 'engine',
                    'Values': [rdsengine]
                }
            ]
        )

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle * 2)
            return get_instance_size(instance, throttle)
        else:
            pprint("Error getting aws instance data:" + str(e))
            sys.exit(1)
    else:
        return awsInstanceData["DBInstances"][0]["DBInstanceClass"]


def checkInstanceSmallerThanMaster(master, list_replicas):
    try:
        message = ""
        for each_rep in list_replicas:
            if bigger(each_rep["size"], master["size"]):
                sendToSlack(each_rep, master)
                message = master["instance"] + " - " + master["size"] + " to instance " + each_rep["instance"] + \
                          " - " + each_rep["size"] + "\n"
        return message

    except Exception as e:
        pprint("Error:" + str(e))
        sys.exit(1)


def processInstances(docdbInstances):
    """

    Recorro las instancias en búsqueda de replicas.
    Luego, guardo datos actuales.
    """
    message = ""
    for i in docdbInstances["DBClusters"]:
        dbInstancesInCluster = i["DBClusterMembers"]
        list_replicas = []
        for son in dbInstancesInCluster:
            temp_dict = {}
            temp_dict["instance"] = son["DBInstanceIdentifier"]
            temp_dict["size"] = get_instance_size(son["DBInstanceIdentifier"])
            if son["IsClusterWriter"]:
                master = temp_dict
            if not son["IsClusterWriter"]:
                list_replicas.append(temp_dict)
        message += checkInstanceSmallerThanMaster(master, list_replicas)
    if message:
        opsg = opsgenie.Opsgenie()
        opsg.set_message("[docdb-replica-bigger-than-master] [{}] List of docdb's with wrong sizes".format(fullprofile))
        opsg.set_description("[Suggest] Evaluar si hace falta un failover o reducir el size de la replica \n\n"
                             "Listado: \n\n{}".format(message))
        opsg.set_alias("docdb-replica-bigger-than-master-{}".format(str(fullprofile)))
        opsg.set_details({"script-path": os.path.realpath(__file__), "user": getpass.getuser()})
        opsg.create()

        #body = "Please perform failover for the following instances <br><br>\n\n"
        #body += message
        #subject = 'DocDB Master Instances with bigger replica'
        #gm = gmail.Gmail('rodri.cadaval@gmail.com')
        #to = "dba-alerts@X1X.opsgenie.net"
        #to = "rodri.cadaval@gmail.com"
        #gm.send_message(to, subject, body)
        #print('Email sent!')


def getDocDBRRList(rdsengine):
    """
  Get list of all the DBs from DocDB
  """

    rdsclient = boto3.client('docdb')
    paginator = rdsclient.get_paginator('describe_db_clusters')
    filter_list = []
    filter_list.append({'Name': 'engine', 'Values': [rdsengine]})
    pages = paginator.paginate(Filters=filter_list)

    for page in pages:
        processInstances(page)

    print()
    return pprint("Finished at {}".format(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


def main():
    getDocDBRRList(rdsengine)


if __name__ == '__main__':
    main()

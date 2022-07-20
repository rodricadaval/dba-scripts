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
import psycopg2
import re
from pprint import pprint
import sys
import argparse
import shelve
import time
import shutil

os.system('clear')

parser = argparse.ArgumentParser(description='Downscale DocDB Replica instances')

parser.add_argument('-a', '--account', action="store", dest="awsaccount", default="xx",
                    help="AWS Account: xx, xxx, x1, x2, x3", required=True)
parser.add_argument('-r', '--region', action="store", dest="awsregion", default="xx-west-2",
                    help="Name of the aws credential profile", required=True)
parser.add_argument('-e', '--engine', action="store", dest="dbengine", default="docdb", help="postgres, mysql or docdb")
parser.add_argument('--nightshift-cass', action="store_true", dest="nightshift", help="flag")
parser.add_argument('--return-original-cass', action="store_true", dest="return_original", help="flag")
parser.add_argument('--check-autoscaling-state', action="store_true", dest="checking", help="flag")
parser.add_argument('--regenerate-shelve', action="store_true", dest="recreate", help="flag")
parser.add_argument('-t', '--tags', action="store", dest="tags", default="none", help="Tag values")


args = parser.parse_args()

awsaccount = args.awsaccount
awsregion = args.awsregion
rdsengine = args.dbengine

tags = args.tags

if args.nightshift:
    action = "downscale"
elif args.return_original:
    action = "upscale"
elif args.checking:
    action = "check"
elif args.recreate:
    action = "recreate_shelve"
else:
    action = "revision"

fullprofile = awsaccount + '-' + awsregion
pprint("Region: " + fullprofile)
# pprint(idata)
boto3.setup_default_session(profile_name=fullprofile)
reducedClass = 'db.t3.medium'

slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXX"
slackChannel = "dba_test_service_autoscaling"


def sendToSlack(awsdata, shelvedata, state, type):
    """
    Slack
    """

    if state in ["success", "performing", "still-in-progress"] and type == "upscale" :
        targetclass=shelvedata["originalClass"]
    elif state in ["success", "performing", "still-in-progress"] and type == "downscale" :
        targetclass = shelvedata["reducedClass"]
    elif type == "check" :
        if state in ["still-in-progress", "success"] :
            targetclass=shelvedata["actualClass"]
        else :
            targetclass="ERROR"
    else :
        targetclass = "ERROR"

    if state == "success":
        color="#36a64f"
        thumb="https://icon-library.com/images/icon-success/icon-success-17.jpg"
    elif state == "error":
        color="#FF164f"
        thumb="https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"
    else:
        color="#111FFF"
        thumb="https://cdn.onlinewebfonts.com/svg/img_506219.png"


    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", "pretext": "' + str.upper(state) + ' ' + str.upper(type) + ' para ' + awsdata["DBInstances"][0]["DBInstanceIdentifier"] + '", \
            "author_name": "Instance Description", \
            "fields": [ \
            {"title": "Original Class", "value": "' + str(shelvedata["originalClass"]) + '", "short": true}, \
            {"title": "Reduced Class", "value": "' + str(shelvedata["reducedClass"]) + '","short": true}, \
            {"title": "Expected Class","value": "' + targetclass + '","short": true}, \
            {"title": "Status","value": "' + str(awsdata["DBInstances"][0]["DBInstanceStatus"]) + '","short": true}, \
            {"title": "Region","value": "' + str(shelvedata["region"]) + '","short": true}, \
            {"title": "AWS Account","value": "' + str(awsaccount) + '","short": true}, \
            {"title": "Cluster","value": "' + awsdata["DBInstances"][0]["DBClusterIdentifier"] + '","short": true} \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "last change", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(shelvedata["timeChangedClass"]) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-DocDBAutoScaling", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


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

def performDownscale(inst, awsdata, shelvedata,throttle=0.2):
    try:
        if not awsdata["DBInstances"][0]["DBInstanceClass"] == shelvedata["reducedClass"]:
            rdsclient = boto3.client('docdb')
            rdsclient.modify_db_instance(
                DBInstanceIdentifier=inst,
                DBInstanceClass=shelvedata["reducedClass"],
                ApplyImmediately=True
            )
            return 'performing'
        else:
            return 'ntd'
    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return performDownscale(inst, awsdata, shelvedata, throttle*2)
        else:
            pprint("Error modifying aws instance class:" + str(e))
            sys.exit(1)

def performUpscale(inst, awsdata, shelvedata, throttle=0.2):
    try:
        if not awsdata["DBInstances"][0]["DBInstanceClass"] == shelvedata["originalClass"]:
            rdsclient = boto3.client('docdb')
            rdsclient.modify_db_instance(
                DBInstanceIdentifier=inst,
                DBInstanceClass=shelvedata["originalClass"],
                ApplyImmediately=True
            )
            return 'performing'
        else:
            return 'ntd'
    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return performUpscale(inst, awsdata, shelvedata, throttle*2)
        else:
            pprint("Error modifying aws instance class:" + str(e))
            sys.exit(1)

def persistInstanceInPerformanceFTWShelve(instance):
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
            time.sleep(0.4)
            return persistInstanceInPerformanceFTWShelve(instance)
        else:
            pprint("Error getting aws instance data:" + str(e))
            sys.exit(1)

    try:
        d = shelve.open("/tmp/shelvepftw_" + awsaccount + "_" + awsregion + "_" + tags.replace("=", "_").replace(",", "_"), writeback=True)

    except Exception as e:
        pprint("Error opening shelve file:" + str(e))
        sys.exit(1)

    now = time.time()
    instname = instance + "-" + awsregion
    instanceExists = instname in d
    #del d[instname]

    if instanceExists:
        idata = {}
        idata = d[instname]
        if action == "revision":
            pprint("Instance exists in shelve: " + instname)
            print()
            pprint("Value ->")
            pprint(d[instname])
            print()
            pprint("Instance real status: " + awsInstanceData["DBInstances"][0]["DBInstanceStatus"])
            print()
        elif action == "recreate_shelve":
            pprint("Recreating shelve for: " + instname)
            if awsInstanceData["DBInstances"][0]["DBInstanceClass"] != idata["reducedClass"]:
                idata["originalClass"] = awsInstanceData["DBInstances"][0]["DBInstanceClass"]
                idata["actualClass"] = awsInstanceData["DBInstances"][0]["DBInstanceClass"]
                idata["clusterIdentifier"] = awsInstanceData["DBInstances"][0]["DBClusterIdentifier"]
                idata["status"] = awsInstanceData["DBInstances"][0]["DBInstanceStatus"]
                idata["dbInstanceArn"] = awsInstanceData["DBInstances"][0]["DBInstanceArn"]
                idata["availabilityZone"] = awsInstanceData["DBInstances"][0]["AvailabilityZone"]
                idata["endpointAddress"] = awsInstanceData["DBInstances"][0]["Endpoint"]["Address"]
                idata["promotionTier"] = awsInstanceData["DBInstances"][0]["PromotionTier"]
                idata["reported"] = 0
                d[instname] = idata
                d.close()
                print()
                pprint("Value ->")
                pprint(idata)
                print()
            else:
                sendToSlack(awsInstanceData, idata, 'error', action)
        elif action == "check":
            pprint("Checking if instance "+instname+" state is correct")
            if idata["actualClass"] == awsInstanceData["DBInstances"][0]["DBInstanceClass"] \
                    and awsInstanceData["DBInstances"][0]["DBInstanceStatus"] == "available":
                if idata["reported"] not in [2, 4]:
                    sendToSlack(awsInstanceData, idata, 'success', action)
                idata["reported"] = 2
                d[instname] = idata
                d.close()
            elif idata["actualClass"] != awsInstanceData["DBInstances"][0]["DBInstanceClass"] \
                    and awsInstanceData["DBInstances"][0]["DBInstanceStatus"] == "modifying":
                if idata["reported"] in [0, 3]:
                    sendToSlack(awsInstanceData, idata, 'still-in-progress', action)
                idata["reported"] = 1
                d[instname] = idata
                d.close()

            else:
                if idata["reported"] in [0, 1, 4]:
                    sendToSlack(awsInstanceData, idata, 'error', action)
                idata["reported"] = 4
                d[instname] = idata
                d.close()

        elif action == "downscale":
            try:
                #if not awsInstanceData["DBInstances"][0]["DBInstanceClass"] == idata["originalClass"]:
                    #idata["originalClass"] = awsInstanceData["DBInstances"][0]["DBInstanceClass"]
                if performDownscale(instance, awsInstanceData, idata) == "performing":
                    sendToSlack(awsInstanceData, idata, 'performing', 'downscale')
                    idata["actualClass"] = idata["reducedClass"]
                    idata["timeChangedClass"] = now
                    idata["reported"] = 0
                    print()
                    pprint("Performing downscale to instance: " + instname + " with target class " + idata[
                        'reducedClass'])
                else:
                    pprint('nothing to do on -> ' + instname)
                d[instname] = idata
                d.close()
            except Exception as e:
                pprint("Error performing downscale to : " + instname + ")")
                pprint("Error message:" + str(e))
                sendToSlack(awsInstanceData, idata, 'error', 'downscale')
        elif action == "upscale":
            try:
                if performUpscale(instance, awsInstanceData, idata) == "performing":
                    sendToSlack(awsInstanceData, idata, 'performing', 'upscale')
                    idata["actualClass"] = idata["originalClass"]
                    idata["timeChangedClass"] = now
                    idata["reported"] = 3
                    print()
                    pprint("Performing upscale to instance: " + instname + " with target class " + idata["originalClass"])
                else:
                    pprint('nothing to do on -> ' + instname)
                d[instname] = idata
                d.close()
            except Exception as e:
                pprint("Error performing upscale to : " + instname + ")")
                pprint("Error message:" + str(e))
                sendToSlack(awsInstanceData, idata, 'error', 'upscale')
    else:
        if awsInstanceData["DBInstances"][0]["DBInstanceClass"] != reducedClass:
            idata = {}
            idata["originalClass"] = awsInstanceData["DBInstances"][0]["DBInstanceClass"]
            idata["reducedClass"] = reducedClass
            idata["actualClass"] = awsInstanceData["DBInstances"][0]["DBInstanceClass"]
            idata["timeChangedClass"] = now
            idata["clusterIdentifier"] = awsInstanceData["DBInstances"][0]["DBClusterIdentifier"]
            idata["status"] = awsInstanceData["DBInstances"][0]["DBInstanceStatus"]
            idata["region"] = awsregion
            idata["dbInstanceArn"] = awsInstanceData["DBInstances"][0]["DBInstanceArn"]
            idata["availabilityZone"] = awsInstanceData["DBInstances"][0]["AvailabilityZone"]
            idata["endpointAddress"] = awsInstanceData["DBInstances"][0]["Endpoint"]["Address"]
            idata["promotionTier"] = awsInstanceData["DBInstances"][0]["PromotionTier"]
            idata["reported"] = 0

            d[instname] = idata
            d.close()

def checkClusterTags(cluster_data):
    """
    Chequeo que los tags coincidan con los propios de la instancia
    """

    instance_arn = cluster_data["DBClusterArn"]

    try:
        rdsclient = boto3.client('docdb')
        aws_instance_tags = rdsclient.list_tags_for_resource(
            ResourceName=instance_arn
        )

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(0.4)
            return checkClusterTags(cluster_data)
        else:
            pprint("Error getting aws instance data:" + str(e))
            sys.exit(1)

    else:
        returned_tags = aws_instance_tags['TagList']
        returned_tags = formatAwsTags(returned_tags)
        return compareTags(returned_tags)


def processInstances(docdbInstances):
    """

    Recorro las instancias en búsqueda de replicas.
    Luego, guardo datos actuales.
    """

    for i in docdbInstances["DBClusters"]:
        dbInstancesInCluster = i["DBClusterMembers"]
        if tags != "none":
            if checkClusterTags(i):
                for son in dbInstancesInCluster:
                    if not son["IsClusterWriter"]:
                        persistInstanceInPerformanceFTWShelve(son["DBInstanceIdentifier"])
        else:
            for son in dbInstancesInCluster:
                if not son["IsClusterWriter"]:
                    persistInstanceInPerformanceFTWShelve(son["DBInstanceIdentifier"])


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

def getDocDBRRList(rdsengine):
    """
  Get list of all the DBs from RDS
  """

    if not os.path.exists("shelve-bkps"):
        os.makedirs("shelve-bkps")
    if os.path.isfile("/tmp/shelvepftw_" + awsaccount + "_" + awsregion + "_" + tags.replace("=", "_").replace(",", "_") + ".db"):
        shutil.copy2("/tmp/shelvepftw_" + awsaccount + "_" + awsregion + "_" + tags.replace("=", "_").replace(",", "_") + ".db", "shelve-bkps/shelvepftw_"
                     + awsaccount + "_" + awsregion + "_" + tags.replace("=", "_").replace(",", "_")+".db." + str(time.time()))

    pprint("Using Shelve: shelvepftw_" + awsaccount + "_" + awsregion + "_" + tags.replace("=", "_").replace(",", "_"))

    rdsclient = boto3.client('docdb')
    paginator = rdsclient.get_paginator('describe_db_clusters')
    filter_list = []
    filter_list.append({'Name': 'engine', 'Values': [rdsengine]})
    pages = paginator.paginate(Filters=filter_list)

    for page in pages:
        processInstances(page)

    print()
    return pprint("Finished")


def main():
    getDocDBRRList(rdsengine)


if __name__ == '__main__':
    main()

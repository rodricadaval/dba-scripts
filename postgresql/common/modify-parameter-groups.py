#!/usr/bin/python3
#
# Rodrigo Cadaval
# rodri.cadaval@gmail.com
#
# Change paramameter groups globaly per region.
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

parser = argparse.ArgumentParser(description='Modifying RDS Parameter Groups')

parser.add_argument('-a', '--account', action="store", dest="awsaccount", default="xx",
                    help="AWS Account: xx, xxx, x1, x2, x3", required=True)
parser.add_argument('-r', '--region', action="store", dest="awsregion", default="xx-west-2",
                    help="Name of the aws credential profile", required=True)
parser.add_argument('-e', '--engine', action="store", dest="dbengine", default="postgres", help="postgres, mysql or docdb", required=True)
parser.add_argument('-p', '--parameter_groups', action="store", dest="parameters", help="Parameter values", required=True)
parser.add_argument('-t', '--tags', action="store", dest="tags", default="none", help="Tag values")
parser.add_argument('-i', '--db-instance-identifier', action="store", dest="instance_identifier", help="Instance")


args = parser.parse_args()

awsaccount = args.awsaccount
awsregion = args.awsregion

if args.dbengine in ["postgres", "mysql"]:
    service = "rds"
elif args.dbengine == "docdb":
    service = "docdb"
else:
    service = args.dbengine

awsengine = args.dbengine
tags = args.tags
parameters = args.parameters
parameters_modified = []

if args.instance_identifier:
    instance_identifier = args.instance_identifier

fullprofile = awsaccount + '-' + awsregion
pprint("Region: " + fullprofile)
boto3.setup_default_session(profile_name=fullprofile)

slackUrl = "https://hooks.slack.com/services/XXXXXXXX/XXXXXXXXXX/XXXXXXXXXXXXXXXXXXX"
slackChannel = "dba_boto3_reports"


def sendToSlack(parameter, state):
    """
    Slack
    """

    if state == "success":
        color="#36a64f"
        thumb="https://icon-library.com/images/icon-success/icon-success-17.jpg"
    elif state == "error":
        color="#FF164f"
        thumb="https://www.nicepng.com/png/full/225-2255762_error404-error-404-icono-png.png"
    else:
        color="#111FFF"
        thumb="https://cdn.onlinewebfonts.com/svg/img_506219.png"

    parameters_formatted = formatParameters()

    context_addition = ''
    for param in parameters_formatted:
        context_addition = ''.join([context_addition, ',{"title": "' + param["ParameterName"] + '", "value": "' + param["ParameterValue"] + '", "short": true}'])

    contextBlock = '"attachments": [{ "mrkdwn_in": ["text"], "color": "' + color + '", "pretext": "' + str.upper(state) + ' modifying parameter group ' + parameter  + '", \
            "author_name": "Summary", \
            "fields": [ \
            {"title": "Parameter Group", "value": "' + parameter + '", "short": true}' + context_addition + ' \
            ], \
            "thumb_url": "' + thumb + '", \
            "footer": "action", \
            "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png", \
            "ts": ' + str(time.time()) + ' }]'

    payloadSlack = '{"channel" : "' + slackChannel + '", "username" : "DBA-Boto3-Jobs", ' + contextBlock + '}'

    try:
        r = requests.post(slackUrl, data=payloadSlack)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        sys.exit(1)


def applyChange(parameter, throttle=0.2):
    try:
        rdsclient = boto3.client(service)
        parameter_list = formatParameters()

        if len(parameter_list) >= 1:
            response = rdsclient.modify_db_parameter_group(
                DBParameterGroupName=parameter,
                Parameters=parameter_list
            )
            sendToSlack(parameter, 'success')

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return applyChange(parameter, throttle*2)
        else:
            pprint("Error modifying aws parameter values:" + str(e))
            sendToSlack(parameter, 'error')
            sys.exit(1)

def persistParameterValues(parameter, throttle=0.2):
    try:
        rdsclient = boto3.client(service)
        awsParamaterData = rdsclient.describe_db_parameter_groups(
            DBParameterGroupName=parameter
        )

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return persistParameterValues(parameter, throttle*2)
        else:
            pprint("Error getting aws parameter data:" + str(e))
            sendToSlack(parameter, 'error')
            sys.exit(1)
    else:
        applyChange(parameter)


def checkInstanceTags(instance_data, throttle=0.2):
    """
    Chequeo que los tags coincidan con los propios de la instancia
    """

    instance_arn = instance_data["DBInstanceArn"]

    try:
        rdsclient = boto3.client(service)
        aws_instance_tags = rdsclient.list_tags_for_resource(
            ResourceName=instance_arn
        )

    except Exception as e:
        if "Throttling" in str(e):
            time.sleep(throttle)
            return checkInstanceTags(instance_data, throttle*2)
        else:
            pprint("Error getting aws instance tags:" + str(e))
            sys.exit(1)

    else:
        returned_tags = aws_instance_tags['TagList']
        returned_tags = formatAwsTags(returned_tags)
        return compareTags(returned_tags)


def processInstances(rdsInstances):
    """

    Recorro las instancias en búsqueda de sus parameter groups.
    Luego, guardo datos actuales.
    """

    for i in rdsInstances["DBInstances"]:
        parameter_group = i["DBParameterGroups"][0]["DBParameterGroupName"]

        if tags != "none":
            if checkInstanceTags(i):
                if parameter_group.split(".")[0] != "default":
                    if parameter_group not in parameters_modified:
                        parameters_modified.append(parameter_group)
                        applyChange(parameter_group)
        else:
            if parameter_group.split(".")[0] != "default":
                if parameter_group not in parameters_modified:
                    parameters_modified.append(parameter_group)
                    applyChange(parameter_group)



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


def formatParameters():
    """
    Genera el array de tags
    """
    array_parameters = parameters.split(";")
    parameter_list = []
    for each_param in array_parameters:
        name = str(each_param).split("=")[0].lower()
        value = str(each_param).split("=")[1]
        parameter_list.append({'ParameterName': name, 'ParameterValue': value, 'ApplyMethod': 'pending-reboot'})

    return parameter_list


def scanInstances():
    """
  Get list of all the DBs from RDS
  """
    rdsclient = boto3.client(service)
    paginator = rdsclient.get_paginator('describe_db_instances')
    filter_list = []
    filter_list.append({'Name': 'engine', 'Values': [awsengine]})
    if "instance_identifier" in globals():
        pages = paginator.paginate(DBInstanceIdentifier=instance_identifier, Filters=filter_list)
    else:
        pages = paginator.paginate(Filters=filter_list)

    for page in pages:
        processInstances(page)

    print()
    return pprint("Finished")


def main():
    scanInstances()


if __name__ == '__main__':
    main()


import os
import requests
import json
import boto3
import yaml
import subprocess
import argparse
import pprint
import sys
from pathlib import Path

accountslist = ["COUNTRY1","COUNTRY2","DEV"]
regionslist = ["xx-east-1","xx-east-2","xx-west-1","xx-west-2"]
dyexport = "/tmp"
dyprefix = "anansiblevar_inventory_ms"

parser = argparse.ArgumentParser(description='Find MS map values')

parser.add_argument('-n', '--name', action="store", dest="varname", default=False, help="Example: user_x_host")
parser.add_argument('-v', '--value', action="store", dest="varvalue", default=False, help="Value (Database, user, host)")
parser.add_argument('-e', '--endpoint', action="store", dest="endpoint", default=False, help="CNAME")
parser.add_argument('-m', '--microservice', action="store", dest="microservice", default=False, help="MS name for example: my-test-ms")
parser.add_argument('-r', '--repoms', action="store", dest="msrequirements", default="MY/MS-REQUIREMENTS/PATH", help="path to ms requirements repo local clone")
parser.add_argument('-c', '--country', action="store", dest="pais", default="all", help="Example: co")

args = parser.parse_args()

varname = args.varname
varvalue = args.varvalue
endpoint = args.endpoint
microservice = args.microservice
msrequirements = args.msrequirements
pais = args.pais

def scanDynamo(fullprofile, table, elementvalue, campo='value', comparador='='):
  #print("\nTabla"+table)
  boto3.setup_default_session(profile_name=fullprofile)
  client = boto3.client('dynamodb')
  if comparador == 'CONTAINS':
    response = client.scan(
       TableName=table.strip(),
      ExpressionAttributeValues={
          ':a': {
              'S': elementvalue,
          },
      },
      ExpressionAttributeNames={
      '#variable_value': campo,
          },
      FilterExpression='contains(#variable_value, :a)'
    )
  else:
    response = client.scan(
       TableName=table.strip(),
      ExpressionAttributeValues={
          ':a': {
              'S': elementvalue,
          },
      },
      ExpressionAttributeNames={
      '#variable_value': campo,
          },
      FilterExpression='#variable_value = :a'
    )

  if response["Count"] != 0:
    return response["Items"]
  else:
    return False

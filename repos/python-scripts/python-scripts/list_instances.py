#!/usr/local/bin/python3
import boto3
from libs.settings import Settings as s
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from libs.rds import RDSClient
from pprint import pprint

# client = boto3.client('rds')
# response = client.describe_db_instances()
# x=',';
# for r in response['DBInstances']:
#   db_name = r['DBName']
# db_instance_name = r['DBInstanceIdentifier']
# db_type = r['DBInstanceClass']
# db_storage = r['AllocatedStorage']
# db_engine = r['Engine']
# print(db_instance_name,db_type,db_storage,db_engine)

rdscli = RDSClient(country="cc")
response = rdscli.get_rds_all_instances()

for r in response['DBInstances']:
    # db_name = r['DBName']
    db_instance_name = r['DBInstanceIdentifier']
    db_type = r['DBInstanceClass']
    db_storage = r['AllocatedStorage']
    db_engine = r['Engine']
    print(db_instance_name, db_type, db_storage, db_engine)

# for instance in rds.instances.all():
# print instance.id, instance.state

#!/usr/bin/python3

import argparse
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
import getpass
from itertools import dropwhile
import logging
import os.path
import psycopg2
import re
import signal
import sys
import time
import pprint

# xx -> Main Account
#   xx-east-1
#   xx-west-2
# xx1 -> Secondary Account
#   xxx-east-1
# dev -> Staging
#   xxxx-east-1

def customLogger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(screen_handler)

    return logger

def main():
    global logger

    args = paramParser()

    api_config = Config(
        region_name = args.region,
        connect_timeout = 5,
        read_timeout = 60,
        retries = {
            'max_attempts': 50,
            'mode': 'standard'
        }
    )
    aws_profile = args.account + '-' + args.region
    session = boto3.session.Session(profile_name=aws_profile)
    rds = session.client('rds', config=api_config)

    if "list" in args.command:
        try:
            paginator = rds.get_paginator('describe_db_instances')
            page_iterator = paginator.paginate(
                Filters=[
                        {
                            'Name': 'engine',
                            'Values': [args.engine]
                        },
                    ]
            )
            for page in page_iterator:
                for i in page['DBInstances']:                  
                    if 'ReadReplicaSourceDBInstanceIdentifier' not in i and i['DBInstanceStatus'] == 'available' and not i['PendingModifiedValues']:
                        logger.info("DBInstanceIdentifier: %s\tBackupRetentionPeriod: %d\tDBInstanceStatus: %s\tEngine: %s" % (i['DBInstanceIdentifier'], i['BackupRetentionPeriod'], i['DBInstanceStatus'], args.engine))
        except Exception as e:
            logger.error("AWS API error: %s", e)
            sys.exit(10)  
    elif "set" in args.command:
        try:
            paginator = rds.get_paginator('describe_db_instances')
            page_iterator = paginator.paginate(
                Filters=[
                        {
                            'Name': 'engine',
                            'Values': [args.engine]
                        },
                    ]
            )
            for page in page_iterator:
                for i in page['DBInstances']: 
                    if 'ReadReplicaSourceDBInstanceIdentifier' not in i and i['DBInstanceStatus'] == 'available' and not i['PendingModifiedValues']:
                        response = rds.modify_db_instance(
                            DBInstanceIdentifier=i['DBInstanceIdentifier'],
                            BackupRetentionPeriod=args.backup_retention_period,
                            PreferredBackupWindow="08:00-09:00",
                            PreferredMaintenanceWindow="mon:09:00-mon:09:30",
                            ApplyImmediately=True
                        )

                        if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
                            logger.info("DBInstanceIdentifier: %s\tBackupRetentionPeriod: %d" % (response['DBInstance']['DBInstanceIdentifier'], response['DBInstance']['BackupRetentionPeriod']))
                        else:
                            logger.error("DBInstanceIdentifier: %s\tHTTPStatusCode: %d" % (response['DBInstance']['DBInstanceIdentifier'], response['ResponseMetadata']['HTTPStatusCode']))
        except Exception as e:
            logger.error("AWS API error: %s", e)
            sys.exit(20)  

def paramParser():
    parser = argparse.ArgumentParser(description='modify RDS params', add_help=True, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", help="You must choose an option")

    # get current config
    list_parse = subparsers.add_parser('list', help="set new param")
    list_parse.add_argument("-a", "--account", help="AWS Account: xx, xxx, dev", default="dev", type=str, required=True)
    list_parse.add_argument("-r", "--region", help="Name of the aws credential profile" , default="xxx-west-2", type=str, required=True)
    list_parse.add_argument("-e", "--engine", help="postgres, mysql or docdb" , default="postgres", type=str, required=False)

    # set new config
    set_parse = subparsers.add_parser('set', help="set new BackupRetentionPeriod")
    set_parse.add_argument("-a", "--account", help="AWS Account: xx, xx2, dev, xx1", default="dev", type=str, required=True)
    set_parse.add_argument("-r", "--region", help="Name of the aws credential profile" , default="xxx-west-2", type=str, required=True)
    set_parse.add_argument("-e", "--engine", help="postgres, mysql or docdb" , default="postgres", type=str, required=False)
    set_parse.add_argument("--backup-retention-priod", help="set the backup period" , default="6", type=int, required=True)

    if len(sys.agv)==1:
        parser.print_help(sys.stderr)
        sys.exit(100)

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    logger = customLogger( os.path.basename(__file__) )
    main()

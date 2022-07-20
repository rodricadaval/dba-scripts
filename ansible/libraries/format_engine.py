#!/usr/bin/python3
import dbm
import os
import re
import shelve
import shutil
import subprocess
import sys
import time
import boto3
import psycopg2
import urllib.parse
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib.settings import Settings as s
from bson import json_util
import json
from botocore.exceptions import ClientError
from inspect import getmembers
from pprint import pprint
from libraries import rds_conf as rds
from libraries import ec2_conf as ec2


class FormatService:

    def __init__(self, service: str):
        self.service = service.lower()
        if self.service in ['rds']:
            self.conf_service = rds.RDSConf()
        elif self.service in ['ec2']:
            self.conf_service = ec2.EC2Conf()

    def format_important_data(self, instance):
        return self.conf_service.get_conf(instance)

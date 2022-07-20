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


class RDSConf:

    def get_conf(self, data: dict):
        return {"host": data["Endpoint"]["Address"], "port": data["Endpoint"]["Port"],
                "name": data["DBInstanceIdentifier"], "class": data["DBInstanceClass"]}
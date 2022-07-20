#!/usr/local/bin/python3.7
#
# Rodrigo Cadaval
# rodri.cadaval@acompanydomain.com
#
# Open DocDB connections inside tmux

import json
import pprint
import sys
import subprocess
sys.path.insert(1, '/home/rodri.cadaval/venv/lib/python3.7/site-packages')
import boto3

mongopassfile = "~/.mongopass"

def sendCommand(cmd):
  try:
    result = subprocess.run([cmd], capture_output=True, check=True, timeout=300, shell=True)
  except Exception as e:
    #print(e)
    pass
  else:
    print(result.stdout)

def createTmuxSession():
  cmd = "sudo tmux new-session -ds docdbnew"
  sendCommand(cmd)

def createTmuxWindow(name, endpoint, password):
  cmd = "sudo tmux new-window -t docdbnew -n "+name+" mongo --username adminuser --password "+str(password)+" --authenticationDatabase admin --host "+endpoint+" --port 27017"
  sendCommand(cmd)

def findAdminPass(name):
  contains_digit = any(map(str.isdigit, name))
  if contains_digit:
    name = name[:-1]

  try:
    cmd = "grep -m1 "+name+" "+mongopassfile
    result = subprocess.run([cmd], capture_output=True, check=True, timeout=300, shell=True)
  except Exception as e:
    pass
  else:
    result = result.stdout.rstrip()
    result = str(result).split("adminuser:")
    return result[1][:-1]

def getDocDBList(fullprofile):
  """
  Get list of all the DocDBs
  """

  boto3.setup_default_session(profile_name=fullprofile)
  try:
    docdbclient = boto3.client('docdb')
  except Exception as e:
    print(e)
    return

  paginator = docdbclient.get_paginator('describe_db_instances')
  try:
    pages = paginator.paginate(Filters=[
        {
          'Name': 'engine',
          'Values': ['docdb']
        }])
  except Exception as e:
    print(e)
    return

  try:
    for page in pages:
      for i in page["DBInstances"]:
        docdbname = i["DBInstanceIdentifier"].split(".")
        print(docdbname[0])
        docdbnaddress = i["Endpoint"]["Address"]
        password = findAdminPass(docdbname[0])
        createTmuxWindow(docdbname[0],docdbnaddress, password)
  except Exception as e:
    print(e)
    return

def killOldTmuxSession():
  cmd = "sudo tmux kill-session -t docdb"
  sendCommand(cmd)

def renameTmuxSession():
  cmd = "sudo tmux rename-session -t docdbnew docdb"
  sendCommand(cmd)

def main():
  createTmuxSession()
  
  allprofilelist = ["xx-xx-west-2","xx-xx-west-1","xx-xx-east-2","xx-xx-east-1","xx1-xx-west-2","x1-xx-west-1","x1-xx-east-2","xx1-xx-east-1"]
  for fullprofile in allprofilelist:
    print(fullprofile)
    getDocDBList(fullprofile)

  killOldTmuxSession()
  renameTmuxSession()

if __name__ == '__main__':
  main()
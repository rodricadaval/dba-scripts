import json
import os
import sys
from datetime import datetime, timedelta
git_root = os.popen("git rev-parse --show-toplevel").read().splitlines()[0]
sys.path.insert(0, git_root + '/repos/python-scripts')
from lib import opsgenie

def close_listed_alerts(opsg, fields='responders=dba and status!=closed'):
    query = fields
    try:
        list_response = opsg.alert_api.list_alerts(limit=50, offset=1, sort='createdAt', order='desc',
                                                   search_identifier_type='name', query=query)
        for alert in list_response.data:
            difference = datetime.now() - alert.created_at.replace(tzinfo=None)
            if difference.days >= 1:
                opsg.close_alert(alert.id)
    except Exception as err:
        print("Exception when calling AlertApi->list_alerts: %s\n" % err)

def main():
    opsg = opsgenie.Opsgenie()
    close_listed_alerts(opsg, "responders=dba and status!=closed")


if __name__ == '__main__':
    main()
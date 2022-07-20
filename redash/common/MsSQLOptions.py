import json
import pyodbc
import sys
import urllib
import os
from pprint import pprint
from EndpointConnectivity import EndpointConnectivity


class MsSQLOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=1433, charset="UTF-8", tds_version="7.0") -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['charset'] = charset
        options['db'] = p_database
        options['password'] = p_secret
        options['port'] = p_port
        options['server'] = p_host
        options['tds_version'] = tds_version
        options['user'] = p_user

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            inst = EndpointConnectivity(p_host, p_port)
            inst.check_conn()

            conn_string = ("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format(
                "{ODBC Driver 17 for SQL Server}", p_host, p_database, p_user, urllib.parse.quote(p_secret)))
            conn = pyodbc.connect(conn_string)
            print("Connection with credentials: OK")
            conn.close()
        except Exception as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def get_endpoint(self, host):
        return str(os.popen("nslookup " + host + " | grep Name | cut -d: -f2 | xargs").readline().strip())

    def same_host(self, p_host, p_database, options):
        return self.get_endpoint(p_host) == self.get_endpoint(options['host']) and p_database in options['db']
import json
import sys
from pprint import pprint
import socket
import psycopg2
import os
from EndpointConnectivity import EndpointConnectivity


class PostgreSQLOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=5432) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['dbname'] = p_database
        options['host'] = p_host
        options['user'] = p_user
        options['password'] = p_secret
        options['port'] = p_port

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            inst = EndpointConnectivity(p_host, p_port)
            inst.check_conn()

            conn = psycopg2.connect(
                dbname=p_database, user=p_user, port=p_port, host=p_host, password=p_secret)
            print("Connection with credentials: OK")
            conn.close()
        except psycopg2.Error as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)

    def get_endpoint(self, host):
        return str(os.popen("nslookup " + host + " | grep Name | cut -d: -f2 | xargs").readline().strip())

    def same_host(self, p_host, p_database, options):
        return self.get_endpoint(p_host) == self.get_endpoint(options['host']) and p_database in options['dbname']
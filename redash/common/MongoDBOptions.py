import json
import sys
import os
import urllib
from pprint import pprint
import pymongo
from EndpointConnectivity import EndpointConnectivity
import traceback


class MongoDBOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=27017) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['connectionString'] = 'mongodb://{}:{}@{}:{}'.format(p_user, urllib.parse.quote(p_secret), p_host, p_port)
        options['dbName'] = p_database

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            inst = EndpointConnectivity(p_host, p_port)
            inst.check_conn()

            conn_string = 'mongodb://{}:{}@{}:{}'.format(p_user, urllib.parse.quote(p_secret), p_host, p_port)
            conn = pymongo.MongoClient(conn_string, serverSelectionTimeoutMS=3000)
            conn.server_info()
            print("Connection with credentials: OK")
            conn.close()
        except pymongo.errors.OperationFailure as e:
            raise
        except Exception as e:
            if "conn" in locals():
                conn.close()
            raise

    def get_host_from_connstring(self, connstring):
        return str(connstring.split("@")[1]).split(":")[0]

    def get_endpoint(self, host):
        return str(os.popen("nslookup " + host + " | grep Name | cut -d: -f2 | xargs").readline().strip())

    def same_host(self, p_host, p_database, options):
        return self.get_endpoint(p_host) in self.get_endpoint(self.get_host_from_connstring(options['connectionString'])) \
               and p_database in options['dbName']


    #def testGettingCollections(self, conn, database):
        #pprint(conn.get_database(database).collection_names())
        #pprint(conn.get_database(database).list_collection_names())

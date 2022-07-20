import json
import sys
from pprint import pprint
from EndpointConnectivity import EndpointConnectivity
from elasticsearch import Elasticsearch


class ElasticSearchOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=9243) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['server'] = "{}:{}/".format(p_host, p_port)
        options['basic_auth_user'] = p_user
        options['basic_auth_password'] = p_secret

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            inst = EndpointConnectivity(p_host, p_port)
            inst.check_conn()

            conn_string = 'https://{}:{}@{}:{}'.format(p_user, p_secret, p_host, p_port)
            conn = Elasticsearch([conn_string])
            print("Connection with credentials: OK")
            conn.close()
        except Exception as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)
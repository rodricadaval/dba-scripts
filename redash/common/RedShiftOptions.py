import json
import redshift_connector
import sys
import urllib
from pprint import pprint
from EndpointConnectivity import EndpointConnectivity

class RedShiftOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=5439) -> str:
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

            conn = redshift_connector.connect(
                database=p_database, user=urllib.parse.quote(p_user), port=p_port, host=p_host, password=urllib.parse.quote(p_secret))
            print("Connection with credentials: OK")
            conn.close()
        except redshift_connector.Error as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)
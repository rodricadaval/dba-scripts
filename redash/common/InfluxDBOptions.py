import json
from influxdb import InfluxDBClient
import sys
from pprint import pprint
from EndpointConnectivity import EndpointConnectivity


class InfluxDBOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=8086) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['url'] = 'https+influxdb://{}:{}@{}:{}/{}?ssh_verify=false'.format(p_user, p_secret, p_host, p_port, p_database)

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            inst = EndpointConnectivity(p_host, p_port)
            inst.check_conn()

            conn = InfluxDBClient(host=p_host, port=p_port, username=p_user, password=p_secret, database=p_database,
                                  ssl=True, verify_ssl=True)
            conn.ping()
            print("Connection with credentials are OK")
            conn.close()
        except Exception as e:
            pprint("Error connecting to database:" + str(e))
            if "conn" in locals():
                conn.close()
            sys.exit(1)
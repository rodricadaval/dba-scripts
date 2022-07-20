import json
from prometheus_api_client import PrometheusConnect
import sys
from pprint import pprint
from EndpointConnectivity import EndpointConnectivity


class PrometheusOptions:
    """
    The Subsystem can accept requests either from the facade or client directly.
    In any case, to the Subsystem, the Facade is yet another client, and it's
    not a part of the Subsystem.
    """

    def get_options(self, p_database, p_host, p_user, p_secret, p_port=80) -> str:
        self.check_conn(p_database, p_host, p_user, p_secret, p_port)

        options = {}
        options['url'] = 'http://{}:{}@{}/{}'.format(p_user, p_secret, p_host, p_database)

        return options

    def check_conn(self, p_database, p_host, p_user, p_secret, p_port):
        """
        Check correct connection
        """
        try:
            inst = EndpointConnectivity(p_host, p_port)
            inst.check_conn()

            p_url = 'http://{}:{}@{}/{}'.format(p_user, p_secret, p_host, p_database)
            conn = PrometheusConnect(url=p_url, disable_ssl=True)
            if conn.check_prometheus_connection():
                print("Connection with credentials: OK")
            else:
                raise Exception()
        except Exception as e:
            pprint("Error connecting to database:" + str(e))
            sys.exit(1)
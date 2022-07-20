import json
import sys
from pprint import pprint
import socket


class EndpointConnectivity:
    """
    This class tries to check peering from redash/dbatools server to the specified endpoint on the
    port related to the engine by default (or given in p_port parameter)
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def check_conn(self):
        """
        Check correct connection
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            sock.connect((self.host, self.port))
            sock.close()
        except OSError:
            print("Server not reachable")
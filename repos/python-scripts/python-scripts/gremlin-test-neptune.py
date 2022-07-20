from __future__  import print_function  # Python 2/3 compatibility

from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

graph = Graph()

remoteConn = DriverRemoteConnection('wss://dev-neptune.cluster-xxxxxxxqual.xx-east-1.neptune.amazonaws.com:8182/gremlin', 'g')
g = traversal().withRemote(remoteConn)

print(g.V().limit(2).toList())
remoteConn.close()

remoteConn = DriverRemoteConnection('wss://dev-neptune.cluster-xxxxxxxqual.xx-east-1.neptune.amazonaws.com:8182/gremlin', 'g')
g = traversal().withRemote(remoteConn)

print(g.V().limit(2).toList())
# IMPORTANT: REMEMBER to CHANGE the cluster endpoint below, in the Python code

from __future__  import print_function  # Python 2/3 compatibility
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# Establish a graph traversal object, by calling the graph's traversal method
# - more at: www.kelvinlawrence.net/book/PracticalGremlin.html
graph = Graph()

neptune_db_writer_endpoint = 'airline-stats-database-1.cluster-cu5yee1e8zxg.us-east-1.neptune.amazonaws.com' 

remoteConn = DriverRemoteConnection(f'wss://{neptune_db_writer_endpoint}:8182/gremlin','g')
g = graph.traversal().withRemote(remoteConn)

# Vertices for some group:
count_airports = g.V().hasLabel('airport').count().toList()
print('airport count: ' + str(count_airports))

# All Flight conections:
count_connections  = g.E().hasLabel('flight_connection').count().toList()
print('connection count: ' + str(count_connections))

# # Show some Edges:
# edges  = g.E().hasLabel('flight_connection').limit(3).valueMap(True).toList()
# print(edges)

# # Show a random Vertex
# vertex = g.V('13485').limit(1).valueMap(True).toList()
# print(vertex)

remoteConn.close()

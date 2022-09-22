# IMPORTANT: REMEMBER to CHANGE the cluster endpoint below (neptune_db_writer_endpoint), in the Python code

from __future__ import print_function  # Python 2/3 compatibility
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# Establish a graph traversal object, by calling the graph's traversal method
# - more at: www.kelvinlawrence.net/book/PracticalGremlin.html

neptune_db_writer_endpoint = 'airline-stats-database-1.cluster-cu5yee1e8zxg.us-east-1.neptune.amazonaws.com' 

graph = Graph()
remoteConn = DriverRemoteConnection(
    f'wss://{neptune_db_writer_endpoint}:8182/gremlin', 'g')
g = graph.traversal().withRemote(remoteConn)

# Print some random pre-existing Vertices in the DB.
print(g.V().limit(2).toList())
remoteConn.close()
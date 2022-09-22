# Relevant notes:
#
#   - Code example to show how to read files in Amazon S3, to push data to Neptune in bulk.
#   - Functions, reusability, error handling and Logger still needs to be implemented.
#   - Values are hard-coded, since this code is meant to serve as initial code snippets only. 
##################################################################################################################################################################

from neptune_python_utils.bulkload import BulkLoad
from neptune_python_utils.gremlin_utils import GremlinUtils
from neptune_python_utils.endpoints import Endpoints
import time
import traceback

# Connection and Endpoint Definitions:
GremlinUtils.init_statics(globals())

endpoints = Endpoints(neptune_endpoint='airline-stats-database-1.cluster-cu5yee1e8zxg.us-east-1.neptune.amazonaws.com')
gremlin_utils = GremlinUtils(endpoints)

conn = gremlin_utils.remote_connection()
g = gremlin_utils.traversal_source(connection=conn)

# Vertices to load:
s3_graph_vertices_data_file = ['s3://acd-bayarea-2022-graphdb-dev/neptune-flight-stats-graph-data/vertices/part-00000-113cc600-1532-4ad0-afb4-f7ff9858a1bb-c000.csv']

# Edges to load:
s3_graph_edges_data_file = ['s3://acd-bayarea-2022-graphdb-dev/neptune-flight-stats-graph-data/edges/part-00000-b5de2886-3803-44bd-92ec-dc051739a3ed-c000.csv']

# Bulk-loading
def neptune_loader(endpoint: object, files_to_load: object, iam_role: object) -> object:
    try:
        for file in files_to_load:
            # Bulk-loading
            bulkload = BulkLoad(
                endpoints=endpoints,
                role=iam_role,
                region='us-east-1',
                source=file,
                update_single_cardinality_properties=True)
            load_status = bulkload.load_async()

            # Checking status:
            status, json = load_status.status(details=True, errors=True)
            print(json)
            load_status.wait()

    except Exception as e:
        print('Error raised: {}'.format(e))
        print(traceback.format_exc())


# Load data Vertices:
neptune_loader(
    endpoint=endpoints,
    files_to_load=s3_graph_vertices_data_file,
    iam_role='arn:aws:iam::507922848584:role/stats-neptune-iam-role-1'
)

# Load data Edges:
neptune_loader(
    endpoint=endpoints,
    files_to_load=s3_graph_edges_data_file,
    iam_role='arn:aws:iam::507922848584:role/stats-neptune-iam-role-1'
)
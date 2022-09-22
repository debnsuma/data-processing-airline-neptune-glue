# Graph Database Modelling for Airport Operator

Instead of utilizing rows and columns in tables, graph data modeling allows users to define an arbitrary domain as a linked graph of nodes and relationships with properties and labels. Graph representation of data has several advantages over relational databases. 

A graph database may be searched by edge type or by traversing the complete graph. Because the associations between nodes are not computed at query time but are stored in the database, traversing the joins or relationships in graph databases is relatively fast. Graph databases are useful for use cases like social networking, recommendation engines, and fraud detection when you need to construct linkages between data and query these associations with minimum processing time.

## What we are going to learn ? 

In this project, the requirement from our customer (simulated), an Airport Operator, is to have an analytics platform using Graph Modeling to answer questions like:

- Which are the best and worst performer airlines for flight delays?

- Which are the most congested airports on the ground (a.k.a. "taxiing")?

- Which are the busiest airport connections?


## Phase 1 (Data Preparation)

1. Clone this repo

2. Extract the data 

``` 
$ cd data-processing-airline-neptune-glue/dataset
$ python unzip.py 
$ cd ..
```

This will create a folder called `extracted` under the `dataset` folder and it will extract all the dataset. 

3. Craete an S3 bucket 

```
$ aws s3api create-bucket --bucket <bucket-name>

```

4. Copy extracted dataset to S3 bucket (s3://<bucket-name>/flight-performance-raw-csv/)

```
$ aws s3 cp extracted/ s3://<bucket-name>/flight-performance-raw-csv/ --recursive

```

5. Create a sample dataset and uploaded it back to S3  (We would need in the next step)

```
$ head -200 'extracted/p_year=2018/p_month=01/Jan_2018.csv' > Jan_2018_sample.csv
$ aws s3 cp Jan_2018_sample.csv s3://<bucket-name>/flight-performance-raw-csv-sample/
```


## Phase 2 (Data Processing)

1. Create a crawler in AWS Glue Crawler (let's say the crawler name is `flight_crawler`)

2. Run the crawler `flight_crawler`

3. Once that is done, go to Amazon Athena and query the data and explore the dataset

```sql

SELECT * FROM "flight_db"."flight_performance_raw_csv"
WHERE p_year = '2021' 
    AND p_month = '01'
limit 10;

```

4. Create a AWS Glue Dev Endpoint using Console 

    - Name of the Glue Dev Endpiont : 	flights-glue-endpoint 

5. Create a Glue Notebook
    - Name of the Glue Notebook     : flights-stats-nb-1
    - IAM Role name                : flights-stats-nb-1

6. Open the Glue Notebook 

- Create a folder : 
    - Data Engineering - Extracting columns for Neptune
- Inside the folder copy this notebook: 
    - Notebook is located under the `code` folder 
    - Name of the notebook `Data Engineering Lab 1.ipynb`

7. Run the notebook to create the `Edges` and `Vertices` files 
    - Edges:    s3://<bucket-name>/neptune-flight-stats-graph-data/edges/
    - Vertex:   s3://<bucket-name>/neptune-flight-stats-graph-data/vertices/

8. Once this is done, we can destroy the Glue Dev Endpoint 

9. Run a `Glue` Job with the following parameters 

    - `--s3_all_data_location` 
        - `s3://<bucket-name>/flights-performance-parquet/`

    - `--s3_edges_location`  
        - `s3://<bucket-name>/neptune-flight-stats-graph-data/edges/`

    - `--s3_file_full_path`                             
        - `s3://<bucket-name>/flight-performance-raw-csv/p_year=2021/p_month=03/`

    - `--input_last_days_to_filter`                     
        - `1`

    - `--s3_vertices_location`                         
        - `s3://<bucket-name>/neptune-flight-stats-graph-data/vertices/`

    - `--transform_all_data_flag`
        - `0`                      
 

10. Review the data 
    - Check the data files in the S3 bucket 

## Phase 3 (Create an Amazon Neptune database)

1. Create a security group for the accessing the Neptune database from Cloud9 instance (assuming you are doing it from the Cloud9 Instance)
    - SG Name : `flights-stats-security-group-1`
    - Self reference SG (port- 8182)
    - Add the security group to the Cloud9 instance and the Neptune instance 

2. Create a Neptune database 

    - Name of the DB       : `airline-stats-database-1`
    - Name of the Notebook : `flight-stats-nb-1`
    - Name of the IAM Role : `aireline-stats-1`

3. Edit the Security Group for Neptune Cluster 

    - Go to Neptune Console and select the Neptune Writer Instance and click on Modify 
    - Add the newly created security group `sg-12340682e73401234` 

4. Connect to the Neptune database

```
$ mkdir gremlin-lab-1
$ cd data-processing-airline-neptune-glue/gremlin-lab-1
$ pip3 install boto3 gremlinpython requests backoff -t .
```

5.  Change the file  `data-processing-airline-neptune-glue/code/gremlin-conn-test.py`

    - And update the neptune database writer endpoint in the file 

```python

from __future__ import print_function  # Python 2/3 compatibility
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# Establish a graph traversal object, by calling the graph's traversal method
# - more at: www.kelvinlawrence.net/book/PracticalGremlin.html

neptune_db_writer_endpoint = '<ADD YOUR ENDPOINT>' 

graph = Graph()
remoteConn = DriverRemoteConnection(
    f'wss://{neptune_db_writer_endpoint}:8182/gremlin', 'g')
g = graph.traversal().withRemote(remoteConn)

# Print some random pre-existing Vertices in the DB.
print(g.V().limit(2).toList())
remoteConn.close()

```

6. Install Amazon Neptune Python Utils Installation 

```
$ cd /home/ec2-user/environment/data-processing-airline-neptune-glue
$ git clone https://github.com/awslabs/amazon-neptune-tools.git
$ cd amazon-neptune-tools/neptune-python-utils
```

7. Edit the build.sh file with the right Python Version 
    - Check the python version 
    - Run `python -V`
    - Edit `build.sh` 
    - Run `sh build.sh`

8. Copy the module folder to the gremlin lab path and append the path to `PYTHONPATH` env variable 

```
$ cd /home/ec2-user/environment/data-processing-airline-neptune-glue/gremlin-lab-1

$ cp -r /home/ec2-user/environment//data-processing-airline-neptune-glue/amazon-neptune-tools/neptune-python-utils/neptune_python_utils .

$ echo 'export PYTHONPATH="/home/ec2-user/environment/data-processing-airline-neptune-glue/gremlin-lab-1"' >> ~/.bashrc 

$ source ~/.bashrc

$ rm -rf /home/ec2-user/environment/data-processing-airline-neptune-glue/amazon-neptune-tools
```

## Phase 4 (Bulk load from S3 to Neptune)

1. Create IAM Role + S3 Endpoint for Python Bulk Loader

    - Follow this : https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html
    - Role name : stats-neptune-iam-role-1
    - Go to the IAM role and edit the trust relationship 
        ```json
        {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": [
                "rds.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
            }
        ]
        }
        ```

2. Add the Role to the Neptune cluster 
    - Follow [this](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html) via Neptune cluster UI

3. Creating the Amazon S3 VPC Endpoint
    - Refer [here](https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html) via VPC UI 
        - Name : `my-s3-endpoint-for-neptune`
        - Service Type : `Gateway`

4. Load the data using Bulk API (Edges and Vertices)

- cd /home/ec2-user/environment/data-processing-airline-neptune-glue/code
- Edit `gremlin-load-bulk.py`
  - Vertices path for S3 : `s3://<bucket-name>/neptune-flight-stats-graph-data/vertices/part-00000-113cc600-1532-4ad0-afb4-f7ff9858a1bb-c000.csv`
  - Edges path for S3 : `s3://<bucket-name>/neptune-flight-stats-graph-data/edges/part-00000-b5de2886-3803-44bd-92ec-dc051739a3ed-c000.csv`
  - Edit `s3_graph_vertices_data_file` variable in the script 
  - Edit `s3_graph_edges_data_file` variable in the script 
  - Neptune endpoint : `airline-stats-database-1.cluster-cu5yee1e8zxg.us-east-1.neptune.amazonaws.com` 
  - Edit `endpoints` variable in the script
  - Edit the `region` in the script (if applicable)
  - Edit the `IAM` role ARN in the script with the role created earlier `stats-neptune-iam-role-1`
- Run `python gremlin-load-bulk.py` 

# Phase 5 (Data query using Python)

1. Count the no. of vertices and edges
    - Edit `gremlin-count-nodes-edges-lab.py` 
    - Change `neptune_db_writer_endpoint` in the script with the Neptune DB endpoint 
    - Run `python gremlin-count-nodes-edges-lab.py`
    - Edit `gremlin-random-queries.py` 
        - Change `neptune_db_writer_endpoint` in the script with the Neptune DB endpoint 
    - Run `python gremlin-random-queries.py`

2. Data Analysis using Neptune Notebook

    - Go to Neptune UI and Open the Notebook 
    - Create a new folder `Neptune Data Visualization`
    - Copy the notebook `code\Neptune Data Querying and Visualization Lab 1.ipynb` inside the `Neptune Data Visualization` folder
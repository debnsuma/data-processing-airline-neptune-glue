#!/usr/bin/env python
# coding: utf-8
# Dataset and metadata details; i.e. column definitions, at the [USA Bureau of Transport Statistics URL](https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr)
import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Other Libs for the ETL
# - Note: UUID added to support ID in EDGEs file
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, to_date, max as _max, date_sub, col


# Spark settings
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)


##################################################################################################################################################################
## Begin: Job Settings and Environment Prep
##################################################################################################################################################################

# Job Parameters
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 's3_file_full_path', 'input_last_days_to_filter', 's3_vertices_location', 
                          's3_edges_location', 'transform_all_data_flag', 's3_all_data_location'])

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# Using Job Parameters:
input_last_days_to_filter = int(args['input_last_days_to_filter'])
s3_file_full_path = args['s3_file_full_path']
s3_vertices_location = args['s3_vertices_location']
s3_edges_location = args['s3_edges_location']

# Job parameter to load all data to Parquet
input_transform_all_data = int(args['transform_all_data_flag'])
if input_transform_all_data == 1:
    s3_all_data_location = args['s3_all_data_location']


##################################################################################################################################################################
## Begin: Data Extraction
##################################################################################################################################################################

# Extract data from Amazon S3. 
#   - Optional: We can load using create_dynamic_frame.from_catalog, to leverage Glue Catalog in a deeper level (out of scope of this training)
df_flight_stats_raw = glueContext.create_dynamic_frame.from_options(
    's3',
    {'paths': [s3_file_full_path]},
    'csv',
    {'withHeader': True}).toDF()


##################################################################################################################################################################
## Begin: Data Filtering for the last N days
##################################################################################################################################################################

# FL_DATE cast data type
df_flight_stats = df_flight_stats_raw.withColumn('FL_DATE',to_date(df_flight_stats_raw.FL_DATE, 'yyyy-MM-dd'))

# Get max date
flight_date_max = df_flight_stats.select(_max('FL_DATE')).first()[0]

# Days to store in graph
last_days_to_filter = timedelta(input_last_days_to_filter)
flight_date_max_minus_n_days = flight_date_max-last_days_to_filter

# Collect last 7 days
df_flight_stats = df_flight_stats.where(col('FL_DATE') >= lit(str(flight_date_max_minus_n_days)))


##################################################################################################################################################################
## Begin: Create time-series graph data for Edges
##################################################################################################################################################################

# Collect columns we require.
df_edges_file = df_flight_stats.select('ORIGIN_AIRPORT_ID','ORIGIN','DEST_AIRPORT_ID','DEST','ORIGIN_CITY_NAME',
                                       'DEST_CITY_NAME',
                                       'MKT_UNIQUE_CARRIER','MKT_CARRIER_FL_NUM','DISTANCE','FL_DATE',
                                       'DEP_DELAY','DEP_DEL15','ARR_DELAY','ARR_DEL15','CANCELLED',
                                       'ACTUAL_ELAPSED_TIME','WEATHER_DELAY','SECURITY_DELAY')

# Simple UDF to generate UUID for the Edges ID
generate_uuid_udf = udf(lambda : str(uuid.uuid4()),StringType())

# Adding ID as UUID user-defined type, a new column with label "route", defined as Integer
df_edges_file = df_edges_file.withColumn('~label', lit('flight_connection'))\
                    .withColumn('~id', generate_uuid_udf())

# Rename columns, to make it Neptune-Gremlin-compatible and more human-readable
df_edges_file = df_edges_file.withColumnRenamed('ORIGIN_AIRPORT_ID','~from')\
                    .withColumnRenamed('DEST_AIRPORT_ID','~to')\
                    .withColumnRenamed('ORIGIN','ORIGIN_CODE:string')\
                    .withColumnRenamed('DEST','DESTINATION_CODE:string')\
                    .withColumnRenamed('ORIGIN_CITY_NAME','ORIGIN_CITY_NAME:string')\
                    .withColumnRenamed('DEST_CITY_NAME','DESTINATION_CITY_NAME:string')\
                    .withColumnRenamed('MKT_UNIQUE_CARRIER','CARRIER_NAME:string')\
                    .withColumnRenamed('MKT_CARRIER_FL_NUM','CARRIER_FLIGHT_NUM:double')\
                    .withColumnRenamed('DISTANCE','DISTANCE:double')\
                    .withColumnRenamed('FL_DATE','FLIGHT_DATE:date')\
                    .withColumnRenamed('DEP_DELAY','DEPARTURE_DELAY:double')\
                    .withColumnRenamed('DEP_DEL15','DEPARTURE_DELAY_15MIN:double')\
                    .withColumnRenamed('ARR_DELAY','ARRIVAL_DELAY:double')\
                    .withColumnRenamed('ARR_DEL15','ARRIVAL_DELAY_15MIN:double')\
                    .withColumnRenamed('CANCELLED','CANCELLED:double')\
                    .withColumnRenamed('ACTUAL_ELAPSED_TIME','ACTUAL_ELAPSED_TIME:double')\
                    .withColumnRenamed('WEATHER_DELAY','WEATHER_DELAY:double')\
                    .withColumnRenamed('SECURITY_DELAY','SECURITY_DELAY:double')


##################################################################################################################################################################
## Begin: Create time-series graph data for Vertices
##################################################################################################################################################################

# Let's not assume that all airports are in ORIGIN airports, so let's do a UNION to bring all DESTINATION airports too
df_vertices_file = df_flight_stats.\
                        select(col('ORIGIN_AIRPORT_ID').alias('~id'),
                                col('ORIGIN').alias('ORIGIN_CODE:string'),
                                col('ORIGIN_CITY_NAME').alias('CITY_NAME:string'))\
                   .union(df_flight_stats.\
                        select(col('DEST_AIRPORT_ID').alias('~id'),
                                col('DEST').alias('DESTINATION_CODE:string'),
                                col('DEST_CITY_NAME').alias('CITY_NAME:string')))\
                   .distinct()

# Adding new column with label "route", defined as Integer
df_vertices_file = df_vertices_file.withColumn('~label', lit('airport'))


##################################################################################################################################################################
## Begin: Load data to Amazon S3, in APPEND mode. Change this (e.g. to "overwrite"), if required.
##################################################################################################################################################################

# Writing Edges data to Amazon S3
df_edges_file = df_edges_file.repartition(1)
df_edges_file.write.mode("append").format("csv").option("header", "true").save(s3_edges_location)

# Writing Vertices data to Amazon S3, to a single file
df_vertices_file = df_vertices_file.repartition(1)
df_vertices_file.write.mode("append").format("csv").option("header", "true").save(s3_vertices_location)

# Write all data to Amazon S3, if requested in Job input params:
if input_transform_all_data == 1:
    df_flight_stats_raw.repartition(1)
    df_flight_stats_raw.write.mode("append").format("parquet").partitionBy('YEAR','MONTH').save(s3_all_data_location)
    

# Commit, especially if Glue bookmarks are used for incremental loads:
job.commit()

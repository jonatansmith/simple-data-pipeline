import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

print('GlueJobETL')
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

##variables
csvPath = 's3a://raw.simple-data-pipeline/input/users/load.csv'
parquetOutputPath = 's3a://stage.simple-data-pipeline/output/users/load.parquet'
##Create dataframe for CSV file
df = spark.read.format('csv').options(header='true', inferSchema='true').load(csvPath)

job.init(args['JOB_NAME'], args)

##Convert DataFrames to AWS Glue's DynamicFrames Object
dynamic_dframe = DynamicFrame.fromDF(df, glueContext, "dynamic_df")

##Write Dynamic Frames to S3 in CSV format. You can write it to any rds/redshift, by using the connection that you have defined previously in Glue
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dynamic_dframe, connection_type = "s3", connection_options = {"path": parquetOutputPath}, format = "parquet", transformation_ctx = "datasink4")

job.commit()

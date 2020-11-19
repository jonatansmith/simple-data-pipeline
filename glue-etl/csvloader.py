import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import row_number



print('GlueJobETL')
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

##variables
csvPath = 's3a://raw.simple-data-pipeline/input/users/load.csv'
#csvPath = 's3a://raw.simple-data-pipeline/input/users/load_force_fail.csv'
parquetOutputPath = 's3a://stage.simple-data-pipeline/output/users/load.parquet'
csvSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("create_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True)
])

##Create dataframe for CSV file
df = spark.read.format('csv').schema(csvSchema).option("header","true").option("timestampFormat",'yyyy-MM-dd HH:mm:ss.SSSSSS').option("mode", "DROPMALFORMED").load(csvPath)
#df.show(2)
#df.printSchema()
job.init(args['JOB_NAME'], args)

#begin deduplication: id is used to distinguish duplicated rows and max updated_date is saved to new df
dfaux = df.select("*", F.row_number().over(Window.partitionBy("id").orderBy(df['update_date'].desc())).alias("row_num"))

dfaux.filter(dfaux.row_num ==1).show()
#deduplicated result dataframe
deduplicated_df= dfaux.filter(dfaux.row_num ==1)

#Preparing data to store in parquet
ndf = deduplicated_df.drop('row_num')
#repartitioning to save single file, for purpose of development
ndf = ndf.repartition(1)

##Convert DataFrames to AWS Glue's DynamicFrames Object
dynamic_dframe = DynamicFrame.fromDF(ndf, glueContext, "dynamic_df")

##Write Dynamic Frames to S3 in Parquet format. 
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dynamic_dframe, connection_type = "s3", connection_options = {"path": parquetOutputPath}, format = "parquet", transformation_ctx = "datasink4")

job.commit()

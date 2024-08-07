import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722707491975 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1722707491975")

# Script generated for node Customer Trusted
CustomerTrusted_node1722707499612 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1722707499612")

# Script generated for node Customer-Accelerometer Filtering
SqlQuery714 = '''
WITH customers_with_accelerometer_data AS (
    SELECT DISTINCT user 
    FROM accelerometer_trusted
)
SELECT DISTINCT t.*
FROM customer_trusted t
JOIN customers_with_accelerometer_data a
ON a.user = t.email
'''
CustomerAccelerometerFiltering_node1722716503577 = sparkSqlQuery(glueContext, query = SqlQuery714, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1722707491975, "customer_trusted":CustomerTrusted_node1722707499612}, transformation_ctx = "CustomerAccelerometerFiltering_node1722716503577")

# Script generated for node customer_curated
customer_curated_node1722717238390 = glueContext.getSink(path="s3://udacity-de-project4/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1722717238390")
customer_curated_node1722717238390.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1722717238390.setFormat("glueparquet", compression="snappy")
customer_curated_node1722717238390.writeFrame(CustomerAccelerometerFiltering_node1722716503577)
job.commit()
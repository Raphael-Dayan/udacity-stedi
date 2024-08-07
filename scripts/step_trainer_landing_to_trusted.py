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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722707491975 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1722707491975")

# Script generated for node Customer Landing
CustomerLanding_node1722707499612 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1722707499612")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1722714559721 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1722714559721")

# Script generated for node Accelerometer Filtering
SqlQuery669 = '''
WITH users_allowed_for_research AS (
    SELECT email
    FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
)
SELECT DISTINCT * 
FROM accelerometer_landing a
JOIN users_allowed_for_research u ON a.user = u.email
'''
AccelerometerFiltering_node1722708481512 = sparkSqlQuery(glueContext, query = SqlQuery669, mapping = {"customer_landing":CustomerLanding_node1722707499612, "accelerometer_landing":AccelerometerLanding_node1722707491975}, transformation_ctx = "AccelerometerFiltering_node1722708481512")

# Script generated for node Customer Filtering
SqlQuery670 = '''
SELECT DISTINCT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
'''
CustomerFiltering_node1722707502679 = sparkSqlQuery(glueContext, query = SqlQuery670, mapping = {"customer_landing":CustomerLanding_node1722707499612, "accelerometer_landing":AccelerometerLanding_node1722707491975}, transformation_ctx = "CustomerFiltering_node1722707502679")

# Script generated for node Step Trainer Filtering
SqlQuery671 = '''
WITH users_allowed_for_research AS (
    SELECT serialnumber
    FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
)
SELECT DISTINCT * 
FROM step_trainer_landing s
JOIN users_allowed_for_research u ON s.serialnumber = u.serialnumber
'''
StepTrainerFiltering_node1722714567436 = sparkSqlQuery(glueContext, query = SqlQuery671, mapping = {"step_trainer_landing":StepTrainerLanding_node1722714559721, "customer_landing":CustomerLanding_node1722707499612}, transformation_ctx = "StepTrainerFiltering_node1722714567436")

# Script generated for node Customer-Accelerometer Filtering
SqlQuery668 = '''
WITH customers_with_accelerometer_data AS (
    SELECT DISTINCT user 
    FROM accelerometer_trusted
)
SELECT DISTINCT *
FROM customer_trusted t
JOIN customers_with_accelerometer_data a
ON a.user = t.email
'''
CustomerAccelerometerFiltering_node1722716503577 = sparkSqlQuery(glueContext, query = SqlQuery668, mapping = {"accelerometer_trusted":AccelerometerFiltering_node1722708481512, "customer_trusted":CustomerFiltering_node1722707502679}, transformation_ctx = "CustomerAccelerometerFiltering_node1722716503577")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1722708718811 = glueContext.getSink(path="s3://udacity-de-project4/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1722708718811")
accelerometer_trusted_node1722708718811.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1722708718811.setFormat("glueparquet", compression="snappy")
accelerometer_trusted_node1722708718811.writeFrame(AccelerometerFiltering_node1722708481512)
# Script generated for node customer_trusted
customer_trusted_node1722707754536 = glueContext.getSink(path="s3://udacity-de-project4/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1722707754536")
customer_trusted_node1722707754536.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1722707754536.setFormat("glueparquet", compression="snappy")
customer_trusted_node1722707754536.writeFrame(CustomerFiltering_node1722707502679)
# Script generated for node step_trainer_trusted
step_trainer_trusted_node1722714569328 = glueContext.getSink(path="s3://udacity-de-project4/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1722714569328")
step_trainer_trusted_node1722714569328.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1722714569328.setFormat("glueparquet", compression="snappy")
step_trainer_trusted_node1722714569328.writeFrame(StepTrainerFiltering_node1722714567436)
# Script generated for node customer_curated
customer_curated_node1722717238390 = glueContext.getSink(path="s3://udacity-de-project4/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1722717238390")
customer_curated_node1722717238390.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1722717238390.setFormat("glueparquet", compression="snappy")
customer_curated_node1722717238390.writeFrame(CustomerAccelerometerFiltering_node1722716503577)
job.commit()
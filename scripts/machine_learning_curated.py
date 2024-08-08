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

# Script generated for node Customer Curated
CustomerCurated_node1723019832001 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1723019832001")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1723017968787 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1723017968787")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1723017963937 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1723017963937")

# Script generated for node SQL Query
SqlQuery805 = '''
SELECT DISTINCT *
FROM accelerometer_trusted a
FULL OUTER JOIN step_trainer_trusted s
    ON s.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1723019857079 = sparkSqlQuery(glueContext, query = SqlQuery805, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1723017963937, "accelerometer_trusted":AccelerometerTrusted_node1723017968787, "customer_curated":CustomerCurated_node1723019832001}, transformation_ctx = "SQLQuery_node1723019857079")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1723019912143 = glueContext.getSink(path="s3://udacity-de-project4/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1723019912143")
MachineLearningCurated_node1723019912143.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1723019912143.setFormat("glueparquet", compression="snappy")
MachineLearningCurated_node1723019912143.writeFrame(SQLQuery_node1723019857079)
job.commit()
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

# Script generated for node Accelerometer Filtering
SqlQuery0 = '''
WITH users_allowed_for_research AS (
    SELECT email, sharewithresearchasofdate
    FROM customer_landing
    WHERE sharewithresearchasofdate IS NOT NULL
)
SELECT DISTINCT a.* 
FROM accelerometer_landing a
JOIN users_allowed_for_research u 
ON a.user = u.email
'''
AccelerometerFiltering_node1722708481512 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":CustomerLanding_node1722707499612, "accelerometer_landing":AccelerometerLanding_node1722707491975}, transformation_ctx = "AccelerometerFiltering_node1722708481512")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1722708718811 = glueContext.getSink(path="s3://udacity-de-project4/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1722708718811")
accelerometer_trusted_node1722708718811.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1722708718811.setFormat("glueparquet", compression="snappy")
accelerometer_trusted_node1722708718811.writeFrame(AccelerometerFiltering_node1722708481512)
job.commit()
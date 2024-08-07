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

# Script generated for node Customer Landing
CustomerLanding_node1722707499612 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1722707499612")

# Script generated for node Customer Filtering
SqlQuery572 = '''
SELECT DISTINCT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
'''
CustomerFiltering_node1722707502679 = sparkSqlQuery(glueContext, query = SqlQuery572, mapping = {"customer_landing":CustomerLanding_node1722707499612}, transformation_ctx = "CustomerFiltering_node1722707502679")

# Script generated for node customer_trusted
customer_trusted_node1722707754536 = glueContext.getSink(path="s3://udacity-de-project4/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1722707754536")
customer_trusted_node1722707754536.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1722707754536.setFormat("glueparquet", compression="snappy")
customer_trusted_node1722707754536.writeFrame(CustomerFiltering_node1722707502679)
job.commit()
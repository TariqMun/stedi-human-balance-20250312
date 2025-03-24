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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1741976268473 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/accelerometer-trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1741976268473")

# Script generated for node customer_trusted
customer_trusted_node1742318407665 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/customer-trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1742318407665")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource1 where email ='Bobby.Habschied@test.com';
'''
SQLQuery_node1742385375245 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource1":customer_trusted_node1742318407665, "myDataSource2":accelerometer_trusted_node1741976268473}, transformation_ctx = "SQLQuery_node1742385375245")

job.commit()

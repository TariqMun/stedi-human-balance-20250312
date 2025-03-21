import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_tusted
accelerometer_tusted_node1741976268473 = glueContext.create_dynamic_frame.from_catalog(database="stedi20250312", table_name="accelerometer_landing", transformation_ctx="accelerometer_tusted_node1741976268473")

# Script generated for node customer_trusted
customer_trusted_node1742318407665 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/customer-trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1742318407665")

# Script generated for node Filtering_customer_data_with_accelerometer_trusted 
SqlQuery0 = '''
select * from myDataSource1 where email in (select user from myDataSource2);

'''
Filtering_customer_data_with_accelerometer_trusted_node1742323047341 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource2":accelerometer_tusted_node1741976268473, "myDataSource1":customer_trusted_node1742318407665}, transformation_ctx = "Filtering_customer_data_with_accelerometer_trusted_node1742323047341")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=Filtering_customer_data_with_accelerometer_trusted_node1742323047341, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742322533979", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1742325102195 = glueContext.getSink(path="s3://stedi-human-balance-20250312/customer-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1742325102195")
customer_curated_node1742325102195.setCatalogInfo(catalogDatabase="stedicust20250320",catalogTableName="customer_curated")
customer_curated_node1742325102195.setFormat("json")
customer_curated_node1742325102195.writeFrame(Filtering_customer_data_with_accelerometer_trusted_node1742323047341)
job.commit()

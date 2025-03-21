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

# Script generated for node customer_trusted
customer_trusted_node1741976268473 = glueContext.create_dynamic_frame.from_catalog(database="stedi20250312", table_name="customer_landing", transformation_ctx="customer_trusted_node1741976268473")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null;
'''
SQLQuery_node1741976983979 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_trusted_node1741976268473}, transformation_ctx = "SQLQuery_node1741976983979")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1741976983979, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741976241374", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1741977347050 = glueContext.getSink(path="s3://stedi-human-balance-20250312/customer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1741977347050")
customer_trusted_node1741977347050.setCatalogInfo(catalogDatabase="stedi20250312",catalogTableName="customer_trusted")
customer_trusted_node1741977347050.setFormat("json")
customer_trusted_node1741977347050.writeFrame(SQLQuery_node1741976983979)
job.commit()

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

# Script generated for node step_trainer_landing
step_trainer_landing_node1741976268473 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/step-trainer-landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1741976268473")

# Script generated for node customer_curated
customer_curated_node1742318407665 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/customer-trusted/run-1742225156828-part-r-00000"], "recurse": True}, transformation_ctx="customer_curated_node1742318407665")

# Script generated for node Filtering_step_trainer_data 
SqlQuery0 = '''
select * from myDataSource1 where serialnumber  in (select serialnumber from myDataSource2);
'''
Filtering_step_trainer_data_node1742323047341 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource1":step_trainer_landing_node1741976268473, "myDataSource2":customer_curated_node1742318407665}, transformation_ctx = "Filtering_step_trainer_data_node1742323047341")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=Filtering_step_trainer_data_node1742323047341, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742322533979", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1742325102195 = glueContext.getSink(path="s3://stedi-human-balance-20250312/step-trainer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1742325102195")
step_trainer_trusted_node1742325102195.setCatalogInfo(catalogDatabase="stedi20250312",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1742325102195.setFormat("json")
step_trainer_trusted_node1742325102195.writeFrame(Filtering_step_trainer_data_node1742323047341)
job.commit()

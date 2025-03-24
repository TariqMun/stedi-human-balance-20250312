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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1741976268473 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/step-trainer-trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1741976268473")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1742318407665 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-20250312/accelerometer-trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1742318407665")

# Script generated for node Filtering_step_trainer_data 
SqlQuery0 = '''
SELECT *
from
    md1 
inner join
    md2 
on cast(md1.sensorReadingtime as bigint)=cast (md2.timestamp as bigint);
'''
Filtering_step_trainer_data_node1742323047341 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"md1":step_trainer_trusted_node1741976268473, "md2":accelerometer_trusted_node1742318407665}, transformation_ctx = "Filtering_step_trainer_data_node1742323047341")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=Filtering_step_trainer_data_node1742323047341, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1742322533979", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1742325102195 = glueContext.getSink(path="s3://stedi-human-balance-20250312/machine-learning-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1742325102195")
machine_learning_curated_node1742325102195.setCatalogInfo(catalogDatabase="stedi20250312",catalogTableName="machine_learning_curated")
machine_learning_curated_node1742325102195.setFormat("json")
machine_learning_curated_node1742325102195.writeFrame(Filtering_step_trainer_data_node1742323047341)
job.commit()

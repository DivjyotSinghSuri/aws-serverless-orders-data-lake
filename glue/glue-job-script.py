import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node Amazon S3
AmazonS3_node1771336784267 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://ecommerce-data-lake-divjyot-dev/clean/"], "recurse": True}, transformation_ctx="AmazonS3_node1771336784267")

# Script generated for node order_Datefilter
order_Datefilter_node1771337600480 = Filter.apply(frame=AmazonS3_node1771336784267, f=lambda row: (bool(re.match("order_date != """, row["order_date"]))), transformation_ctx="order_Datefilter_node1771337600480")

# Script generated for node Change Schema
ChangeSchema_node1771336909549 = ApplyMapping.apply(frame=order_Datefilter_node1771337600480, mappings=[("order_id", "string", "order_id", "string"), ("customer_id", "string", "customer_id", "string"), ("amount", "string", "amount", "double"), ("order_date", "string", "order_date", "string")], transformation_ctx="ChangeSchema_node1771336909549")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1771336909549, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1771336711585", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1771337051497 = glueContext.getSink(path="s3://ecommerce-data-lake-divjyot-dev/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["order_date"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1771337051497")
AmazonS3_node1771337051497.setCatalogInfo(catalogDatabase="orders_analytics_db",catalogTableName="orders_curated")
AmazonS3_node1771337051497.setFormat("glueparquet", compression="snappy")
AmazonS3_node1771337051497.writeFrame(ChangeSchema_node1771336909549)
job.commit()

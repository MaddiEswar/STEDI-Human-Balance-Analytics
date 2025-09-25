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
AmazonS3_node1758777157518 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-work/customer/landing"], "recurse": True}, transformation_ctx="AmazonS3_node1758777157518")

# Script generated for node Applyprivacy
Applyprivacy_node1758777392419 = Filter.apply(frame=AmazonS3_node1758777157518, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Applyprivacy_node1758777392419")

# Script generated for node Trusted Customers
EvaluateDataQuality().process_rows(frame=Applyprivacy_node1758777392419, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777106056", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomers_node1758777515219 = glueContext.write_dynamic_frame.from_options(frame=Applyprivacy_node1758777392419, connection_type="s3", format="json", connection_options={"path": "s3://udacity-work/customer/trusted/", "partitionKeys": []}, transformation_ctx="TrustedCustomers_node1758777515219")

job.commit()

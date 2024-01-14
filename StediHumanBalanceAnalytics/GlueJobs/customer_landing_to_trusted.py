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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1705164443588 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1705164443588",
)

# Script generated for node Shared for Research
SqlQuery0 = """
SELECT
  *
FROM
  customer_landing
WHERE
  shareWithResearchAsOfDate != 0
;
"""
SharedforResearch_node1705203177020 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1705164443588},
    transformation_ctx="SharedforResearch_node1705203177020",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705203305036 = glueContext.getSink(
    path="s3://mattbx17-udacity-stedi/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1705203305036",
)
CustomerTrusted_node1705203305036.setCatalogInfo(
    catalogDatabase="udacity-stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1705203305036.setFormat("json")
CustomerTrusted_node1705203305036.writeFrame(SharedforResearch_node1705203177020)
job.commit()

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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705163776498 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1705163776498",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705205546927 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1705205546927",
)

# Script generated for node Join
SqlQuery0 = """
SELECT
  accelerometer_landing.*
FROM
  accelerometer_landing
INNER JOIN
  customer_trusted
ON
  customer_trusted.email = accelerometer_landing.user
;
"""
Join_node1705205232394 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1705163776498,
        "customer_trusted": CustomerTrusted_node1705205546927,
    },
    transformation_ctx="Join_node1705205232394",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705205682457 = glueContext.getSink(
    path="s3://mattbx17-udacity-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1705205682457",
)
AccelerometerTrusted_node1705205682457.setCatalogInfo(
    catalogDatabase="udacity-stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1705205682457.setFormat("json")
AccelerometerTrusted_node1705205682457.writeFrame(Join_node1705205232394)
job.commit()

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

# Script generated for node Customer Trusted
CustomerTrusted_node1705209847689 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1705209847689",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705209951579 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1705209951579",
)

# Script generated for node Join
SqlQuery0 = """
SELECT
  DISTINCT customer_trusted.*
FROM
  customer_trusted
INNER JOIN
  accelerometer_trusted
ON
  customer_trusted.email = accelerometer_trusted.user
;


"""
Join_node1705210002681 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1705209951579,
        "customer_trusted": CustomerTrusted_node1705209847689,
    },
    transformation_ctx="Join_node1705210002681",
)

# Script generated for node Customer Curated
CustomerCurated_node1705210221097 = glueContext.getSink(
    path="s3://mattbx17-udacity-stedi/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1705210221097",
)
CustomerCurated_node1705210221097.setCatalogInfo(
    catalogDatabase="udacity-stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1705210221097.setFormat("json")
CustomerCurated_node1705210221097.writeFrame(Join_node1705210002681)
job.commit()

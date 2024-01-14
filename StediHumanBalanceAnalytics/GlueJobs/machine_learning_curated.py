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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705241912030 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1705241912030",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705241737925 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1705241737925",
)

# Script generated for node Join
SqlQuery0 = """
SELECT
  *
FROM
  step_trainer_trusted
INNER JOIN
  accelerometer_trusted
ON
  step_trainer_trusted.sensorReadingTime = accelerometer_trusted.timestamp
;

"""
Join_node1705241955513 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1705241912030,
        "accelerometer_trusted": AccelerometerTrusted_node1705241737925,
    },
    transformation_ctx="Join_node1705241955513",
)

# Script generated for node Amazon S3
AmazonS3_node1705242066494 = glueContext.getSink(
    path="s3://mattbx17-udacity-stedi/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1705242066494",
)
AmazonS3_node1705242066494.setCatalogInfo(
    catalogDatabase="udacity-stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1705242066494.setFormat("json")
AmazonS3_node1705242066494.writeFrame(Join_node1705241955513)
job.commit()

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

# Script generated for node Customer Curated
CustomerCurated_node1705240447470 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1705240447470",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1705164685666 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mattbx17-udacity-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1705164685666",
)

# Script generated for node Join
SqlQuery0 = """
SELECT
  step_trainer_landing.*
FROM
  step_trainer_landing
INNER JOIN
  customer_curated
ON
  step_trainer_landing.serialNumber = customer_curated.serialNumber
;

"""
Join_node1705240521770 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_curated": CustomerCurated_node1705240447470,
        "step_trainer_landing": StepTrainerLanding_node1705164685666,
    },
    transformation_ctx="Join_node1705240521770",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705240626546 = glueContext.getSink(
    path="s3://mattbx17-udacity-stedi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1705240626546",
)
StepTrainerTrusted_node1705240626546.setCatalogInfo(
    catalogDatabase="udacity-stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1705240626546.setFormat("json")
StepTrainerTrusted_node1705240626546.writeFrame(Join_node1705240521770)
job.commit()

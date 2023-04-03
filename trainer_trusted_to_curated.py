
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
# StepTrainerTrusted_node1676402404135 ??
# JoinCustomerandStepTrainer_node167643624725 ??

StepTrainerTrusted_node1676402404135 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1676402404135",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Join Customer and Step Trainer
JoinCustomerandStepTrainer_node167643624725 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1676402404135,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="JoinCustomerandStepTrainer_node167643624725",
)

# Script generated for node Drop Fields
DropFields_node1676302768067 = DropFields.apply(
    frame=JoinCustomerandStepTrainer_node167643624725,
    paths=["user"],
    transformation_ctx="DropFields_node1676302768067",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676078606395 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1676302768067,
    database="project",
    table_name="machine_learning_curated",
    transformation_ctx="StepTrainerTrusted_node1676078606395",
)

job.commit()

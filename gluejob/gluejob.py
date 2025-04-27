import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

df=spark.read.format("csv").option("header","True").option("path","s3://27aprfardeen/info.csv").load()
df.write.format("csv").option("header","True").option("s3://27aprfardeen/output").save()
job.init(args['JOB_NAME'], args)
job.commit()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from urllib.parse import urlparse
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("My Spark Application").config("spark.executor.cores", "2").config("spark.pyspark.python3", "py").config("spark.executor.memory", "2g").config("spark.driver.memory", "10g").config("spark.master", "local[4]").getOrCreate()

def DomainExt(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return domain

domainExtractor = udf(DomainExt, StringType())

df =spark.read.parquet("output.parquet")
df = df.withColumn("IsShopify",df["content"].contains("cdn.shopify.com"))
df =df.filter(df['IsShopify']==True)

df = df.withColumn("domain",domainExtractor(df["url"]))
# remove duplicates
df =df.dropDuplicates(["domain"])
df = df.drop("content")
# df.coalesce(1).write.option("header","true").csv("output.csv")
df.show()

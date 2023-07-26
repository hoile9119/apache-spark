# script.py

# inherit site-packages
import site
site.addsitedir("./environment/lib/python3.9/site-packages")

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit

import datetime

conf = SparkConf()
spark = (
        SparkSession.builder
                    .appName("spark-yarn")
                    .config(conf=conf)
                    .enableHiveSupport()
                    .getOrCreate()
)

# xml code

from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import col, expr, decode

def ext_from_xml(xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)

def ext_schema_of_xml_df(df, options={}):
    assert len(df.columns) == 1

    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_xml_module = getattr(getattr(
        spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
    java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
    return _parse_datatype_json_string(java_schema.json())


def process_row(row):
    pass

# Kafka configuration
sasl_mechanism = "SCRAM-SHA-256"
security_protocol = "SASL_SSL"
username = ''
password = ''
broker = ''
kafka_topic = ''

kafka_options = {
    "kafka.bootstrap.servers": broker,
    "kafka.security.protocol": security_protocol,
    "kafka.ssl.check.hostname": True,
    "kafka.ssl.ca.location": "cert.crt",
    "kafka.sasl.mechanism": sasl_mechanism,
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger":"100",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";',
    "subscribe": kafka_topic
}

# Create Kafka consumer DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Process the Kafka DataFrame

output = df\
  .selectExpr("cast(key as string) as key","cast(value as string) as value")

# xml parsing
import json

with open("schema.json", "r") as json_file:
    loaded_data = json.load(json_file)
schema = StructType.fromJson(loaded_data)

pdf = output.withColumn("parsed",ext_from_xml(expr("value"),schema)).selectExpr("parsed.*")

# Define the output query
query = pdf.writeStream \
.outputMode("append") \
.foreach(process_row)\
.option("checkpointLocation", "checkpointLocation") \
.start()

query.awaitTermination()



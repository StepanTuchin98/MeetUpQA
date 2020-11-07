import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType

sql = "SELECT DISTINCT p_a.address, p_a.birthdate, p_a.mail, p_a.name, p_a.username, u_p.scenario " \
      "FROM p_a " \
      "JOIN uuid_paths u_p ON u_p.uuid=p_a.uuid " \
      "WHERE name IS NOT NULL"


def create_spark():
    return SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config('spark.driver.host', '127.0.0.1') \
        .getOrCreate()


def create_schema():
    return StructType([
        StructField('address', StringType(), True),
        StructField('birthdate', DateType(), True),
        StructField('date_time', TimestampType(), True),
        StructField('mail', StringType(), True),
        StructField('name', StringType(), True),
        StructField('sex', StringType(), True),
        StructField('uri_path', StringType(), True),
        StructField('username', StringType(), True),
        StructField('uuid', StringType(), True)

    ])


def read_spark_df_select(spark, schema, folder, filename):
    json_file_path = os.path.join(folder, filename)

    df = spark.read.json(json_file_path, schema=schema, multiLine=True)
    df.printSchema()

    df.createTempView("p_a")
    df.show(20, truncate=False)

    uri_id_df = df.select([c for c in df.columns]) \
        .sort("date_time") \
        .groupBy("uuid") \
        .agg(collect_list(col("uri_path")).alias("scenario"))

    uri_id_df.show(20, False)
    uri_id_df.createTempView("uuid_paths")

    result = spark.sql(sql)
    write_output_csv(folder, result)


def write_output_csv(folder, result):
    csv_path = os.path.join(folder, "result")

    result \
        .withColumn("scenario", col("scenario").cast("string")) \
        .write \
        .option("header", "true") \
        .option("sep", ",") \
        .mode("overwrite") \
        .csv(csv_path)


def run_spark(folder, filename):
    spark = create_spark()
    schema = create_schema()

    read_spark_df_select(spark, schema, folder, filename)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from consumer import consumer_data
import pyspark.sql.functions as func



def preprocessed_data():
    spark = SparkSession.builder.appName('Movie Data PreProcessing').getOrCreate()

    data = consumer_data()
    schema = StructType([StructField("movie_name", StringType(), True),
                         StructField("year", StringType(), True),
                         StructField("collection", StringType(), True)])

    df = spark.createDataFrame(data=data, schema=schema)
    df = df.withColumn('Collections', func.regexp_replace('Collection', '[,]', '').cast('float'))
    df = df.withColumn('year', df.year.cast('integer'))
    df = df.drop('Collection')
    processed_df = df.groupBy('year').agg(sum('Collections').alias("collections")).orderBy('year').toPandas()

    # print(processed_df.show())

    print("Processed Data Analytics with Pyspark...")
    return processed_df


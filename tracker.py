# Create the Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

# Create the Spark Session
spark = (
    SparkSession.builder
    .appName("Transaction Streamin Job") 
    .config("spark.streaming.stopGracefullyOnShutdown", True)
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .master("local[*]")
    .getOrCreate()
)

# Define the schema for the JSON data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

# Create the kafka_df to read from kafka
kafka_df = (
    spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()
)

# Create the value_df to read from kafka    
value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

print(value_df.printSchema())
value_df


# transactions_df = value_df.select("value.*") \
#     .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
#     .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
#     .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

# window_agg_df = trade_df \
#     .groupBy(  # col("BrokerCode"),
#      window(col("CreatedTime"), "15 minute")) \
#     .agg(sum("Buy").alias("TotalBuy"),
#          sum("Sell").alias("TotalSell"))

# output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")


# running_total_window = Window.orderBy("end") \
#     .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# final_output_df = output_df \
#     .withColumn("RTotalBuy", sum("TotalBuy").over(running_total_window)) \
#     .withColumn("RTotalSell", sum("TotalSell").over(running_total_window)) \
#     .withColumn("NetValue", expr("RTotalBuy - RTotalSell"))

# final_output_df.show(truncate=False)

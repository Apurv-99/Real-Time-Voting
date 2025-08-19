import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == "__main__":
    print(f"PySpark Version: {pyspark.__version__}")

    spark = SparkSession.builder \
        .appName("VotingAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])


    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), vote_schema).alias("data")) \
        .select("data.*")

    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party_affiliation", "photo_url").agg(_sum("vote").alias("total_votes"))
    turnout_by_location = enriched_votes_df.groupBy("state").count().withColumnRenamed("count", "voter_count")
    votes_by_gender = enriched_votes_df.groupBy("gender").agg(_sum("vote").alias("total_votes"))


    votes_per_candidate_to_kafka = votes_per_candidate.select(to_json(struct(col("*"))).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "checkpoints/votes_per_candidate") \
        .outputMode("update") \
        .start()

    turnout_by_location_to_kafka = turnout_by_location.select(to_json(struct(col("*"))).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "turnout_by_location") \
        .option("checkpointLocation", "checkpoints/turnout_by_location") \
        .outputMode("update") \
        .start()

    votes_by_gender_to_kafka = votes_by_gender.select(to_json(struct(col("*"))).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "votes_by_gender") \
        .option("checkpointLocation", "checkpoints/votes_by_gender") \
        .outputMode("update") \
        .start()

    print("Spark streams started. Awaiting termination...")
    spark.streams.awaitAnyTermination()
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val conf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[*]")

// Create Spark session
val spark = SparkSession.builder().config(conf).getOrCreate()

val kafkaStream = spark.readStream
  .format("kafka")
  .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="CAKHHIO74VLR7LYK" password="Y05ZLtvCqopUbMkG2LcC24MUWocNuTYVKp1pf1U7YrkjJg9VBS1PBdGuSt3rx+mD";',
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.bootstrap.servers", "pkc-rgm37.us-west-2.aws.confluent.cloud:9092")
  .option("kafka.group.id", "web-events")
  .option("subscribe", "bootcamp-events-prod")
  .option("startingOffsets", "latest")
  .load()

val query = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .outputMode("append")
  .format("console")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

query.awaitTermination()

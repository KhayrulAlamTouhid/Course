// Databricks notebook source
/*
  1. Make Word counting streming programm.
  2. Make streming programe for data take from filestore.


*/

// COMMAND ----------

// MAGIC %md ###Make Word Counting Programme.

// COMMAND ----------

import org.apache.spark.sql.functions._
val df = spark
  .readStream
  .format("socket")
  .option("host","1208-043543-mokx8wsy-10-172-168-205")
  .option("port","3332")
  .load()

// body of streaming.
val words = df.select(explode(split($"value"," ")).as("words"))
val count = words.groupBy($"words").count()

val output = count
  .writeStream
  .format("console")
  .outputMode("complete")
  .start()
output.awaitTermination()

// COMMAND ----------

// MAGIC %md ###make streming programe for data come from file.

// COMMAND ----------

import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

val userSchema = StructType(
  Array(
    StructField("Index", IntegerType, true),
    StructField("User Id", StringType, true),
    StructField("First Name", StringType, true),
    StructField("Last Name", StringType, true),
    StructField("Sex", StringType, true),
    StructField("Email", StringType, true),
    StructField("Phone", StringType, true),
    StructField("Date of birth", StringType, true),
    StructField("Job Title", StringType, true)
  )
)


val df = spark.readStream.schema(userSchema).csv("file:/databricks/driver/data_input")
  

// body of streaming.
val result = df.select("*")

val output = result
  .writeStream
  .option("header","true")
  .format("console")
  .outputMode("append")
  .start()
output.awaitTermination()

// COMMAND ----------

output.show

// COMMAND ----------


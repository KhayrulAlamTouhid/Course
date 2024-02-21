// Databricks notebook source
/*
    1. Marge Dataframe using union.

*/

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// i can marge many way.
val df1 = spark.read.option("header","true").csv("dbfs:/FileStore/tables/union/datafile_1.csv")
val df2 = spark.read.option("header","true").csv("dbfs:/FileStore/tables/union/datafile_3.csv")

// COMMAND ----------

val df2_update = df2.withColumn("asge", lit("unkonwn"))

// COMMAND ----------

//if i use union i have to insure that my both dataframe column are same.
// and which datafram will write left it is show to fast.
val uniontodf = df2_update.unionAll(df1).show

// COMMAND ----------

// my second way. we can marge 2 dataframe.
val schema = StructType(
  Array(
    StructField("ID", IntegerType, true),
    StructField("Name", StringType, false),
    StructField("AGe", DoubleType, true)
  )
)

// COMMAND ----------

val df1 = spark.read.option("header","true").schema(schema).csv("dbfs:/FileStore/tables/union/datafile_1.csv")
val df2 = spark.read.option("header","true").schema(schema).csv("dbfs:/FileStore/tables/union/datafile_3.csv")

// COMMAND ----------

val union2way = df1.union(df2).show

// COMMAND ----------


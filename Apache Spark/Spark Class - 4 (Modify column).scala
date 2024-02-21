// Databricks notebook source


// COMMAND ----------

import org.apache.spark.sql.types._
val define_schema = new StructType(Array(
StructField("id", IntegerType, true),
StructField("first_name", StringType, true),
StructField("last_name", StringType, true),
StructField("country", StringType, true),
StructField("number", StringType, true),
StructField("position", StringType, true),
StructField("SaLary", IntegerType, true),
))

// COMMAND ----------

val employee = spark.read.option("header", "true").schema(define_schema).csv("/FileStore/tables/data.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md ####Create new column to another column.

// COMMAND ----------

val create = employee.withColumn("Bonus", $"SaLary" * 1).alias("bonus")

// COMMAND ----------

create.printSchema

// COMMAND ----------

val total_sum = create.withColumn("Total", $"SaLary" + $"Bonus")

// COMMAND ----------

// MAGIC %md ### some oparetion of lit function

// COMMAND ----------

//if we have to pass just valu than i have to use lit function. and if i don't use lit function than i can't use direct valu
 val data_lit = total_sum.withColumn("test" , lit("nodata")).show

// COMMAND ----------

// MAGIC %md ###replace value

// COMMAND ----------

// when have to change valu or modify valu than we have to use lit function.
create.select($"id", regexp_replace($"first_name", "Lex" , "Alex").as("Name")).where($"id"===102).show
//if i don't use where condition and if some valu is same than every valu can be change.

// COMMAND ----------

//when i will create column and if i have to do space in 2 column than i have to use lit function.
val total_concat = create.withColumn("New", concat($"first_name", lit(" "), $"last_name"))

// COMMAND ----------

// MAGIC %md ###Drop Column

// COMMAND ----------

val drop_column = total_concat.drop($"last_name", $"first_name", $"SaLary")

// COMMAND ----------

val see_column = drop_column.select($"id", $"country", $"number", $"New".alias("Full Name"), $"Bonus")

// COMMAND ----------

// MAGIC %md ## create new column using same column, means i want to modify this column.

// COMMAND ----------

val modify_column = see_column.withColumn("Bonus", ($"Bonus" * .1) + $"Bonus").show(8)

// COMMAND ----------


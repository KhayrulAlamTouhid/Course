// Databricks notebook source
/*
  1. Make dataframe with multiple datafile.
  2. After create dataframe some operations.
  3. Optimization Using Cash and persist.
*/

// COMMAND ----------

//if i want to add multiple datafile in one dataframe.
val make_dataframe_with_multiple_value = spark.read.option("header","true").csv("dbfs:/FileStore/tables/add_more_csv") 
// if i want to add specific file like csv or jason or parquet or any other, i have to use pathGlobFilter is True.
val make_datafrma2 = spark.read.option("header","true").option("pathGlobFilter","*.csv").csv("dbfs:/FileStore/tables/add_more_csv")
// if i want to add every dataset folder and subfolder data, i have to use recursivefilelookup is true.
val make_datafrma3 = spark.read.option("header","true").option("recursiveFileLookup","true").csv("dbfs:/FileStore/tables/multiple_data_csv")

// COMMAND ----------

make_datafrma2.show

// COMMAND ----------

val df = spark.read.option("header","true").csv("dbfs:/FileStore/tables/big_data_concept/people_2000000.csv")

// COMMAND ----------

// Cash() and persist() like similar, cash() don't have choices but persist() have.
// when i will use cash(). Then Data Load from Momory and when i will use persist() then data load from disk.
// if i use cash() i will have not any option, but if i use persist() i have some option that data load from where, example(disk only, memory only, disk and memory)
// when i will use cash() i have one problem, that is immagin i have a huge amount of data, like 500 Mb but my memory size is 300 Mb, than where my full data will be load.
// when i am use that first time, it need lot of time but next you don't need many time. it will be 100 time faster.
// if i want to release memory i have to write unpersist()
val optimize = df.cache

// COMMAND ----------

val operation = optimize.groupBy($"Job Title").count.show(false)

// COMMAND ----------

// when i am using persist(), i have to import storage org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel

// COMMAND ----------

val  df2 = spark.read.option("header","true").csv("dbfs:/FileStore/tables/big_data_concept/organizations_2000000.csv")

// COMMAND ----------

val distinct = df2.select("Country").distinct.show

// COMMAND ----------

distinct.count

// COMMAND ----------

df2.persist(StorageLevel.MEMORY_AND_DISK)

// COMMAND ----------

df2.groupBy("Founded").count.show(false)

// COMMAND ----------

// i can't uncash(), becasue spark haven't function uncash() but spark have unpersist(). when i have to release that things, i have to use unpersisit()
df2.unpersist

// COMMAND ----------

// MAGIC %md #### Check which file come from where

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val df = spark.read.option("header","true").option("recursiveFileLookup","true").csv("dbfs:/FileStore/tables/multiple_data_csv")

// COMMAND ----------

// when i will use this function i have to import function.
val check_file = df.withColumn("Check File", input_file_name)

// COMMAND ----------

// if i need to know how many data come from which file, i have to use group by function.

// COMMAND ----------

display(check_file.groupBy($"Check File").count)

// COMMAND ----------

display(check_file)

// COMMAND ----------


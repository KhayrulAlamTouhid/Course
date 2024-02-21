// Databricks notebook source
/*  1. group by with agg functions.
    2. how to connect spark shell to mysql localy. and also read and write oparetions.
    3. build own spark session.

*/

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val input_data = spark.read.option("header", "true").option("sep","#").option("inferSchema","true").csv("/FileStore/tables/Output")

// COMMAND ----------

val group = input_data.groupBy($"position").agg(sum("salary").as("sumofsalary"), min("salary").as("Minimum"), count("first_name")).show

// COMMAND ----------

val result = input_data.select(
  countDistinct("first_name").as("count"),
  sum("salary").as("sum"),
  sum_distinct($"salary").as("Sum of Salary"),
  max("salary").as("Maximum"),
  min("salary").as("Minimum"),
  avg("salary").as("Average")
).show

// COMMAND ----------

input_data.select(count("id")).show

// COMMAND ----------

input_data.show

// COMMAND ----------

// i can connect spark to mysql many way, this is my first way, this code provide by chatgpt.
/*
val jdbcUrl = "jdbc:mysql://localhost:3306/practice"
val jdbcUsername = "your_username"
val jdbcPassword = "1310380131"
val dbtable = "cricket2"
val driver = "com.mysql.cj.jdbc.Driver"

val connectionProperties = new java.util.Properties()
connectionProperties.setProperty("user", jdbcUsername)
connectionProperties.setProperty("password", jdbcPassword)
connectionProperties.setProperty("driver", driver)

val df = spark.read.jdbc(jdbcUrl, dbtable, connectionProperties)
*/
// and that is my second way, it is provide by course.

first i have to connect spark-shell with "spark-shell --jars C:\my\mysql-connector-j-8.1.0.jar" that is download from mvnrepository.
val read_data_from_mysql = spark.read.format("jdbc")
  .option("url","jdbc:mysql://localhost/practice")  //where do i want to import my data.
  .option("driver","com.mysql.jdbc.Driver")         // what is the through of import data.
  .option("dbtable","cricket2")                     // what is my table name.  
  .option("user","root")                            // what is my user name.
  .option("password","1310380131")                  // what is my password.
  .load                                             // and last what do i want to do.

// when that have to use spark shell that i have to remove this comment. we have to use like that -------val read_data_from_mysql = spark.read.format("jdbc").option("url","jdbc:mysql://localhost/practice").option("driver","com.mysql.jdbc.Driver").option("dbtable","cricket2").option("user","root").option("password","1310380131").load

// COMMAND ----------

// write oparetion also similar to read oparetion.
val read_data_from_mysql = spark.write.format("jdbc")
  .option("url","jdbc:mysql://localhost/practice")  // where do i want to import my data. and what do i want to use which database.
  .option("driver","com.mysql.jdbc.Driver")         // what is the through of import data.
  .option("dbtable","cricket4")                     // what do i want to create what table name.  
  .option("user","root")                            // what is my user name.
  .option("password","1310380131")                  // what is my password.
  .save                                             // and also write save.

  // when that have to use spark shell that i have to remove this comment. we have to use like that -------val read_data_from_mysql = spark.write.format("jdbc").option("url","jdbc:mysql://localhost/practice").option("driver","com.mysql.jdbc.Driver").option("dbtable","cricket4").option("user","root").option("password","1310380131").save

// COMMAND ----------

// how to build own spark session.
// when we have to build my own app than we have to create my own spark session.
// first we have to import spark session.

import org.apache.spark.sql.SparkSession

// COMMAND ----------

val touhid = SparkSession.builder
.appName("test")      // what do i want.
.master("local(*)")   // local(*) means how many CPU core do i want to use.
.getOrCreate          // get means if i already had create than we have to recreate or Create means new create my spark session.

// COMMAND ----------

// MAGIC %md #### Repartition and Coalesce

// COMMAND ----------

val df = touhid.read.csv("dbfs:/FileStore/tables/first_assignment_output")

// COMMAND ----------

//check how many partition in mydata have.
// i have to use rdd for show because getNumPartitions only action for rdd.
df.rdd.getNumPartitions

// COMMAND ----------

// how to increasse my partition. and also this way i can decress my partition.
val df_repartition = df.repartition(6)

// COMMAND ----------

df_repartition.rdd.getNumPartitions

// COMMAND ----------

// coalesce is how to decress my partion. i couldn't incress partition size.
val df_colease = df.coalesce(2)

// COMMAND ----------

df_colease.rdd.getNumPartitions

// COMMAND ----------

// how to do parition and colease work.
val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),4)

// COMMAND ----------

// how to incress partiton.
val rdd2 = rdd.repartition(6)

// COMMAND ----------

rdd2.glom.collect

// COMMAND ----------

// when i will do use partition for decressing partition my partition will be shuffleing for 2 partition.
val rdd3 = rdd.repartition(2)

// COMMAND ----------

rdd3.glom.collect

// COMMAND ----------

// coalesce don't work shuffleing, coalesce work, we have six partition and i want to do two partiton, than if i do use coalesce than first 3 partition marge is one partition and second three partition marge one partition.
val rdd3_coalesc = rdd2.coalesce(2)

// COMMAND ----------


rdd3_coalesc.glom.collect

// COMMAND ----------


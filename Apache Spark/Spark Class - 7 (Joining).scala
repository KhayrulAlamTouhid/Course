// Databricks notebook source
/*  1.inner join.
    2. create database and table using sql.
    3. aggergiation, group by and order by function use in dataframe.
    4. value update using when and otherwise function.
    5. write data from dataframe to DBFS.
    6. write data from dataframe to hive.
    7. make dataframe from hive.
    8. left and Right join.

*/

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val player = spark.read.option("header","true").option("inferSchema","true").csv("dbfs:/FileStore/playerdata.csv")
val over = spark.read.option("header","true").option("inferSchema","true").csv("dbfs:/FileStore/overdata.csv")

// COMMAND ----------

// Inner join - default join is inner join thats why we can write inner join or join both are same.
// left join - left join means grab all value from left table and match value from right table.
// right join - right join means grab all value from right table and match value from left table.
// full join - full join means grab all value from both table, no matter what which value did match or which value did unmatch.
val join = player.join(over).where(player("id") === over("id")).show

// COMMAND ----------

// we can join dataframe many way
val join2= player.join(over, $"playerid"=== $"ballid")
join2.printSchema
// and third number way is....
val join3 = player.join(over, player("id")=== over("id")).select(player("id"),player("name"),player("over")).show

// COMMAND ----------

// MAGIC %sql
// MAGIC create database practice;
// MAGIC show databases;
// MAGIC use practice;

// COMMAND ----------

// MAGIC %sql
// MAGIC -- we can create extarnal table many way, and this is my first way.
// MAGIC -- when i will create extarnal table i have to mantions folder not exect data.
// MAGIC CREATE TABLE IF NOT EXISTS player_null
// MAGIC USING CSV
// MAGIC OPTIONS (
// MAGIC   'path' '/FileStore/tables',
// MAGIC   'header' 'true', -- Set to 'true' if your CSV file has a header row
// MAGIC   'inferSchema' 'true' -- Set to 'true' to infer the schema from the data
// MAGIC );
// MAGIC

// COMMAND ----------

spark.sql("select * from player_null").show

// COMMAND ----------

// MAGIC %sql
// MAGIC -- my second way.
// MAGIC create external table player(playerid int, id int, name string, over string, maiden int) row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1") location 'dbfs:/FileStore/tables';

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from player;

// COMMAND ----------

val df = spark.read.option("header", "true").option("inferSchema","true").csv("dbfs:/FileStore/tables/data.csv")

// COMMAND ----------

//group by position column
val groupby = df.groupBy($"position").count()

// COMMAND ----------

// order by and we did show which position are work the most member.
groupby.orderBy($"count".desc).limit(1).withColumnRenamed("count","member").show

// COMMAND ----------

//sum all salary
val totalSalary = df.agg(sum($"Salary")).show

// COMMAND ----------


val sumsalry = df.groupBy("position").sum("salary").withColumnRenamed("sum(salary)", "Totalsalaryofeverydepartment").show

// COMMAND ----------

//val update = df.withColumn("position", lit("No JOb")).where($"id".isin(101,106,113))
// we can use that but than i can't update every column value with condition.
val update2 = df.withColumn("position", when($"id"===101, "No job").when($"id"===105, "No job").when($"id"===107, "No job").when($"id"===106, "No job").otherwise($"position"))

// COMMAND ----------

update2.write.option("header","true").option("sep","#").csv("/FileStore/tables/Output")
/*
val outputPath = "/FileStore/tables/Output"

// Write the DataFrame to a CSV file
update2.write
  .format("csv")
  .option("header", "true") // Write header
  .mode("overwrite") // Overwrite existing data
  .save(outputPath)
*/

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/Output

// COMMAND ----------

//we have to use separetor # cause my data value separetor is #, if my data separetor is ,. than we haven't to use separetor, because by default separetor is ,.
val input_data = spark.read.option("header", "true").option("sep","#").option("inferSchema","true").csv("/FileStore/tables/Output")

// COMMAND ----------

val hivetable = input_data.where($"position"==="FI_ACCOUNT")

// COMMAND ----------

hivetable.write.saveAsTable("practice.hivetable")

// COMMAND ----------

val createdataframe = spark.sql("select * from practice.hivetable").show

// COMMAND ----------

val df1 = spark.read.option("header","true").csv("dbfs:/FileStore/tables/join_practice/player_join.csv")
val df2 = spark.read.option("header","true").csv("dbfs:/FileStore/tables/join_practice/over_join_left-1.csv")
val df3 = spark.read.option("header","true").csv("dbfs:/FileStore/tables/join_practice/over_join_left.csv")

// COMMAND ----------

//left join.
val join3 = df1.join(df2, df1("id")===df2("id"), "left").show

// COMMAND ----------

//Right join.
val join4 = df1.join(df2, df1("playerid")===df2("id"), "right").show

// COMMAND ----------

//if you want to join as many as dataframe.
//multi join. join with 3 dataframe.
val join5 = df1.join(df2, df1("playerid")===df2("id"), "left")
.join(df3,  df2("id")===df3("ballid"), "left").show

// COMMAND ----------

join5.show

// COMMAND ----------

df1.show()
df2.show()

// COMMAND ----------


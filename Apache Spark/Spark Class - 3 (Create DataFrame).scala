// Databricks notebook source
// MAGIC %md
// MAGIC ###### Create DataFrame.

// COMMAND ----------

//how can i create scema.
//we can define scema other defarents way. 
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
//define schema on deffarent way.
val define_schema2 = new StructType()
.add("id", IntegerType, true)
.add("first name", StringType, true)
.add("last name", StringType, true)
.add("country", StringType, true)
.add("number", StringType, true)
.add("position", StringType, true)
.add("SaLary", IntegerType, true)


// COMMAND ----------

//val create_dataframe = spark.read.option("header", "true").schema(define_schema2).csv("/FileStore/tables/data.csv")
// i can create dataFrame one deffarent way
val create_dataframe2 = spark.read.format("csv").option("header", "true").schema(define_schema).load("/FileStore/tables/data.csv")

// COMMAND ----------

create_dataframe2.show(5)

// COMMAND ----------

// MAGIC %md ## Multiple option use a single options.

// COMMAND ----------

//i can do use multiple option in a single options.
//val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("/FileStore/tables/data.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._
val df = spark.read.option("header", "true").schema("id int, first_name string, last_name string, country string, Numb string, position string, salary int").csv("/FileStore/tables/data.csv")

// COMMAND ----------

// MAGIC %md ##create Temporary view

// COMMAND ----------

df.createOrReplaceTempView("employee")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from employee;

// COMMAND ----------

// MAGIC %sql
// MAGIC --Many deffarent where clause.
// MAGIC select * from employee  where(salary > 15000);
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from employee where first_name like ("%a")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from employee where position in ("AD_VP" , "IT_PROG")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from employee where id between 115 and 120 order by id asc;

// COMMAND ----------

// MAGIC %sql
// MAGIC --i can every command write in a single column.
// MAGIC select id, first_name, position, salary, country from employee where id between 105 and 140  and first_name like "%l" and position in ("FI_ACCOUNT", "PU_CLERK") and salary == 7800;
// MAGIC

// COMMAND ----------

// MAGIC %md #### where clause using programming way.

// COMMAND ----------

spark.sql("""select * from employee where id between 105 and 120 order by id asc""").show

// COMMAND ----------

spark.sql("""select * from employee where last_name like '%l'""").show

// COMMAND ----------

spark.sql("""select * from employee where position in ('SA_MAN', 'ST_CLERK')""").show

// COMMAND ----------

spark.sql("""select * from employee where position in ('SA_MAN', 'ST_CLERK') and last_name like '%l' and id = 145""").show

// COMMAND ----------

df.select($"id", upper($"first_name").alias("name"), upper($"position") as("Position"), $"salary").filter($"position" === "FI_ACCOUNT").show

// COMMAND ----------

df.select($"id", $"first_name", $"position").where($"id".between(102,110)).show

// COMMAND ----------

df.select($"id", $"first_name", $"position").where($"first_name".like("%l")).show

// COMMAND ----------

df.select($"id", $"first_name", $"position").where($"position".isin("FI_ACCOUNT", "MK_MAN")).show

// COMMAND ----------

//using multiple where clause in a single row.
df.select($"id", $"first_name", $"position").where($"position".isin("FI_ACCOUNT", "MK_MAN") && $"first_name".like("%l")).show

// COMMAND ----------

//using multiple where clause on multiple row but in single dataframe.
val multiple_df = df.select($"id", $"first_name", $"position", $"salary", $"country").where($"position".isin("FI_ACCOUNT", "MK_MAN"))

// COMMAND ----------

val new_multiple_df = multiple_df.select($"id", $"first_name", $"country").where($"first_name".like("%l")).show

// COMMAND ----------


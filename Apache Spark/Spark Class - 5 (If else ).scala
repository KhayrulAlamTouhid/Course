// Databricks notebook source
/*
    1. if else statment.
    2. use many if else condition in one column.
    3. when and otherwise statment.
    4. 


*/

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val df = spark.read.option("header", true).option("inferSchema", true).csv("dbfs:/FileStore/tables/data-1.csv")

// COMMAND ----------


val statement = df.select($"id", $"first_name", expr("if(position = 'AD_VP', 'yes', 'no')").as("valu"))

// COMMAND ----------

statement.select($"id", $"first_name", $"valu").where($"id" === 102)

// COMMAND ----------

//if i want to use multiple if condition in single line than i have to use this statment
df.selectExpr("id", "first_name", "if(salary > 5000, 'Rice', 'Poor')" , "if(position = 'AD_VP', 'Advanced', 'ok') as pos")


// COMMAND ----------

//multiple condition in same column
df.select($"id" , $"first_name", 
when($"salary" > 5000 && $"salary" <= 7000, "good salary")
.when($"salary" > 9000, "Avarege")
.otherwise("Very Bjig")
.alias("salary"))

// COMMAND ----------

// if i use $"position" that is return me every row, but if i use "position" that is refar me Just every row will be position.
val mydata = df.select($"id", $"first_name", when($"position" === "IT_PROG", "IT Adminastator").when($"position"=== "FI_ACCOUNT", "DB Officar").otherwise($"position").alias("position")).show



// COMMAND ----------

val newdf = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/tables/job-3.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC modify date of birth column 

// COMMAND ----------

val data = newdf.withColumn("dob", to_date($"dob", "dd=MM=yyyy")).withColumnRenamed("dob", "dateofbirth")


//if my date column like 25**04**2023 that's situations i have to write this statment "dd**mm**yyyy"

// COMMAND ----------

data.select($"dateofbirth",dayofmonth($"dateofbirth").as("day"), month($"dateofbirth").as("month")).show

// COMMAND ----------

/* After formating, if i want to manupulate date than i am to use this.
  * for need Just short name of Day than we have to use "EEE" three Captital E.
  * for need full name of day than we have to use "EEEE" four capital E.
  * for need full name of Month than  we have to use "MMMM" four capital M.
  * for need just short name of month than we have to use "MMM" three captial M.
  * if i need just day month and year just we have to use dd, mm, yyyy.
  * if i need just last 2 digite of year we have to use yy.
*/
data.select(date_format($"dateofbirth", "EEEE, EEE, MMMM, MMM, dd, yyyy, yy").as("dateofbirth")).show(false)

// COMMAND ----------

// to defarents between today date and player date.
val newage = data.select(datediff(current_date, $"dateofbirth")/365 cast "int" as "age").show

// COMMAND ----------

val data2 = data.select($"id",$"name",$"over", date_format($"dateofbirth","EEE") as "day", date_format($"dateofbirth", "EEEE, EEE, MMMM, MMM, dd, yyyy, yy") as "dateofbirth")
data2.select($"id",$"name",$"over", $"day", $"dateofbirth").where($"day"==="Fri").show(false)

// COMMAND ----------

val addcol = df.orderBy($"salary".asc)

// COMMAND ----------


val colformate = df.withColumn("dob", lit("21=12=2023"))
val colformate2 = colformate.withColumn("dateofbirth", lit("21=12=2023 05:11:25 AM"))


// COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

// COMMAND ----------

val timestampcovert = colformate2.withColumn("dateofbirth",unix_timestamp($"dateofbirth", "dd=MM=yy HH:mm:ss a").cast("timestamp"))
val timestampcovert2 = timestampcovert.withColumn("dateofbirth",date_format($"dateofbirth", "yyyy-mm-dd HH:mm:ss a"))

// COMMAND ----------

//val result = timestampcovert2.withColumn("covertdob", expr("right(dateofbirth, 2)")).show(false)
//val result = timestampcovert2.where("right(dateofbirth, 2)='AM'").show(false)
val result = timestampcovert2.withColumn("number", expr("substring(number, 2,5)")).show(false)

// COMMAND ----------

val dataformate = colformate2.withColumn("dob", to_date($"dob", "dd=MM=yyyy"))

// COMMAND ----------

val extractdata = colformate.withColumn("dateofbirth", to_date($"dateofbirth", "dd-mm-yyyy"))
extractdata.select($"id", $"first_name", month($"dateofbirth").as("Month")).show

// COMMAND ----------

val df2 = spark.read.option("header", "true").option("inferSchema" , "true").csv("dbfs:/FileStore/patient.csv")

//val dobformating = df2.withColumn("dob", to_date($"dob", "dd-mm-yyyy"))
//val dobformating2 = df2.select($"lastvisitedate", hour($"lastvistedate"))

// COMMAND ----------

//add month
val addmonth = df2.select($"dob", add_months($"dob", 10), date_add($"dob", 5), date_add($"dob", 5 *365)).show


// COMMAND ----------


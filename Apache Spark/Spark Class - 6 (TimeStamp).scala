// Databricks notebook source
/*  1. convert dateformate.
    2. Covert Timestamp.
    3. use where function on date and also timestamp.
    4. splite time and date on date and timestamp column.
    5. splite string value.
    ************
    1. check null value and notnull value.
    2. drop null value.
    3. cast int column to string column.
    4. update null value.
    5. splite string value. and also cricket over.
*/

// COMMAND ----------

import org.apache.spark.sql.functions._
//if we didn't formate date and timestamp than we have to set legacy.
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

// COMMAND ----------

val timestampp = spark.read.option("header", "true").csv("dbfs:/FileStore/timestamp.csv")
val dobform = timestampp.withColumn("dob", to_date($"dob", "mm/dd/yyyy"))

// COMMAND ----------

dobform.where(year($"dob")===2022 and month($"dob")===1 and $"name".like("s%")).show

// COMMAND ----------

dobform.show

// COMMAND ----------

val admonth = dobform.withColumn("dateofbirth", add_months($"dob",5))
val addday = admonth.withColumn("date_add", date_add($"dob",5))

// COMMAND ----------

//how many way i can filter 
//number 1
addday.where(year($"dob") === 2021).show
//and number 2
addday.select("id", "name", "dob", "lastvisitedate","date_add").where(date_format($"dob", "yyyy")===2021).show
//and number 3
addday.where("left(dob, 4)=2021").show


// COMMAND ----------

//when my data had AM or PM i have to use "dd/MM/yy HH:mm:ss a" i have to use extra a.
//when we need time is international standard time (24 hours formate) like "13:35:24" i have to use for timestamp cast capital of H.(HH) or we need (12 hours formate) than i have to use lower h. (hh)
val timestampconvert = timestampp.withColumn("lastvisitedate", unix_timestamp($"lastvisitedate", "dd/MM/yy HH:mm").cast("timestamp"))

// COMMAND ----------

val timestampfilter = timestampconvert.select($"lastvisitedate", hour($"lastvisitedate").as("Hours"), minute($"lastvisitedate").as("Minute"), second($"lastvisitedate").as("Second")).show
val timestampfilter2 = timestampconvert.select("id", "name","lastvisitedate").withColumn("Hour", hour($"lastvisitedate")).withColumn("Minute", minute($"lastvisitedate")).withColumn("Second", second($"lastvisitedate")).show

// COMMAND ----------

val test = timestampp.withColumn("test", lit("5.7"))

// COMMAND ----------

//val castedvol = test.withColumn("test", col("test").cast("string"))

// COMMAND ----------

//val splitDF = castedvol.withColumn("name_split", split(col("name"), "."))

// COMMAND ----------

val splitDF = test.withColumn("firstName", split(col("lastvisitedate"), ":")(0)).withColumn("lastName", split(col("lastvisitedate"), ":")(1)).show

// COMMAND ----------

//if i create new column to exgesting column with filter.
timestampp.withColumn("new", expr("Right(id,1)")).show

// COMMAND ----------

timestampp.show

// COMMAND ----------

import org.apache.spark.sql.types._
val define_schema = new StructType(Array(
StructField("id", IntegerType, true),
StructField("name", StringType, true),
StructField("dismisal", StringType, true),
StructField("run", IntegerType, true),
StructField("fantasy point", IntegerType, true),
StructField("six", IntegerType, true),
StructField("four", IntegerType, true),
StructField("strict rate", StringType, true)
))

// COMMAND ----------

val not_null = spark.read.option("header", "true").schema(define_schema).csv("dbfs:/FileStore/tables/cricket_null.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ### How to check null valu.

// COMMAND ----------

//if do i want to not show null valu than i have to write this.
//val notNullDF = not_null.where(col("name").isNotNull).show(false)

// if do i want to show null valu than i have to write this.
//val notNullDF2 = not_null.where("name is Null").show


// COMMAND ----------

//drop null valu.
// we can drop null valu many way.
//first if i drop all null value in the table i have to use this statment
//not_null.na.drop("any").show(false)
//second if i want to drop all null valu in the row i have to use this statment.
//not_null.na.drop("all").show(false)
//last if i want to delete specefic cloumn null value than i have to use this statment.
//not_null.na.drop(Seq("id", "name", "run")).show(false)

// COMMAND ----------

//not_null.na.fill("valu not here").show
// if i want to use specific data in specefic coloumn first i have to do insure that column type is string. or i have to do typecast.
val castedDF = not_null.withColumn("run", col("run").cast("string"))
castedDF.na.fill("valu not here" , Seq("run", "dismisal")).show(false)

// COMMAND ----------

val data = spark.read.option("inferSchema","true").option("header","true").csv("dbfs:/FileStore/tables/split.csv")

// COMMAND ----------

//val splitover = data.withColumn("ball", split($"over")),"\\.")(0)
val splitover = data.withColumn("ball1", split(col("over"), "\\.")(0)).withColumn("lastName", split(col("over"), "\\.")(1))

// COMMAND ----------

val coverover = splitover.withColumn("ball1", ($"ball1"*6).cast("integer")).show

// COMMAND ----------


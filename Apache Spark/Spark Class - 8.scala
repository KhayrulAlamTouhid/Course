// Databricks notebook source
/*  1. wirte data one formate to another formate. and mode also overwrite, appand
    2. Make partition.
    3. Frist Assiangment (formate dateofbirth, rename, change column value, drop null data, save data in hive table.)
    4. ceil, floor and round functions.
    5. use aggreate functions.
    6. use group by functions with aggregate functions.

*/

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val df = spark.read.option("header","true").csv("dbfs:/FileStore/tables/cricket_null.csv")

// COMMAND ----------

//write data to parqut,avro,rc,orc to csv formate.
//df.write.parquet("dbfs:/FileStore/tables/cricket_output_parquet")
df.write.option("sep","$").csv("dbfs:/FileStore/tables/cricket_output_csv")

// COMMAND ----------

val df2 = spark.read.option("sep","$").csv("dbfs:/FileStore/tables/cricket_output_csv")
//val df1 = spark.read.parquet("dbfs:/FileStore/tables/cricket_output_parquet")

// COMMAND ----------

//make partition.
val dataframe = spark.read.option("header","true").csv("dbfs:/FileStore/tables/first_assignment.csv")


// COMMAND ----------

// we can use partion as many as column.
dataframe.write.partitionBy("gender").csv("dbfs:/FileStore/tables/first_assignment_output")

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables/first_assignment_output

// COMMAND ----------

val dataframe2 = spark.read.csv("dbfs:/FileStore/tables/first_assignment_output/gender=F")

// COMMAND ----------

dataframe2.na.fill("unknown", Seq("_c0")).show

// COMMAND ----------

dataframe2.show

// COMMAND ----------

val input_data = spark.read.option("header", "true").option("sep","#").option("inferSchema","true").csv("/FileStore/tables/Output").where($"position" isin("No job","ST_MAN"))

// COMMAND ----------

//if i run second time this statment this also return already exists. i have to use mode function.
// if i use overwrite function than my old data will be gone and new data will overwrite this dataframe.
input_data.write.mode("append").parquet("/FileStore/tables/Outputwithpurquet")
// and if i use appand function than my old also stay this dataframe and new data also appand this dataframe.

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/Outputwithpurquet

// COMMAND ----------

val output_to_input = spark.read.option("header", "true").option("inferSchema","true").parquet("dbfs:/FileStore/tables/Outputwithpurquet/part-00000-tid-7189655797914346844-68d47f90-4520-4b6e-9376-0cfb80cd64a2-35-1-c000.snappy.parquet").show

// COMMAND ----------

display(output_to_input)

// COMMAND ----------

// MAGIC %md #### MY first Assingment.

// COMMAND ----------

// my frist assignment.
val df = spark.read.option("header","true").csv("dbfs:/FileStore/tables/first_assignment.csv")

// COMMAND ----------

/*  ****** My first Assignment Task.*******
    1. first formate dob column.
    2. Change gendar F= Female and M= Male.
    3. add Mr. and Mrs. with name value.
    4. drop null value in id and name column.
    5. save table to hive.

// COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

// COMMAND ----------

//formating dob column.
val formate_dob_col = df.withColumn("dob", to_date($"dob", "dd/mm/yyyy")).withColumnRenamed("dob", "dateofbirth")

// COMMAND ----------

//changing gender vlaue.
//val change_gender = formate_dob_col.withColumn("gender", expr("if(gender = 'M', 'male','female')"))
val change_gender_col = formate_dob_col.withColumn("gender", when($"gender"=== "M", "Male").otherwise("Female"))

// COMMAND ----------

// add name with Mr. and Mrs.
val add_name = change_gender_col.withColumn("name", when($"gender"==="Male", concat(lit("Mr."),$"name")).otherwise(concat(lit("Mrs."),$"name")))

// COMMAND ----------

//drop all null value.
val drop_null_value = add_name.na.drop(Seq("id","name"))

// COMMAND ----------

//than my assignment last task is save to hive table.
drop_null_value.write.saveAsTable("first_assignment")

// COMMAND ----------

//concat 2 value.
val concat_vlaue = drop_null_value.withColumn("name", concat($"name",lit(" "), $"gender")).show

// COMMAND ----------

val df = spark.read.option("header","true").csv("dbfs:/FileStore/tables/cricket_null.csv")

// COMMAND ----------

val cleandata = df.na.drop("any")

// COMMAND ----------

cleandata.select($"Field 8", ceil($"Field 8").alias("ceil")).show(5)
cleandata.select($"Field 8", floor($"Field 8").alias("floor")).show(5)
cleandata.select($"Field 8", round($"Field 8").alias("round")).show(5)

// COMMAND ----------

cleandata.show

// COMMAND ----------

import org.apache.spark.sql.functions.{count, sum_distinct}

// COMMAND ----------

val df = spark.read.option("header", "true").option("sep","#").option("inferSchema","true").csv("/FileStore/tables/Output")

// COMMAND ----------

// Count the number of rows
val numRows = df.count()

// Get the list of column names
val numCols = df.columns.length

// Print the number of rows and columns
println(s"Number of rows: $numRows")
println(s"Number of columns: $numCols")


// COMMAND ----------

//val count = input_data.groupBy($"first_name").count().as("count").orderBy($"count".desc).show

// COMMAND ----------

//Using aggregate functions.
//input_data.select(countDistinct($"first_name").as("count"), sum($"salary").as("sum"), sum_distinct($"salary").as("Sum of Salary"), max($"salary").as("Maximum"), min($"salary").as("Minimum"), avg($"salary").as("Average"), count($"first_name")).show

val result = df.select(
  countDistinct("first_name").as("count"),
  sum("salary").as("sum"),
  sum_distinct($"salary").as("Sum of Salary"),
  max("salary").as("Maximum"),
  min("salary").as("Minimum"),
  avg("salary").as("Average")
).show

// COMMAND ----------

//val group = input_data.groupBy($"position").count().alias("count").orderBy($"count".desc).show
// when you have to use sum,min, max than we have to use agg functions
val group = input_data.groupBy($"position").agg(sum("salary").as("sumofsalary"), min("salary").as("Minimum"), countDistinct("first_name")).show


// COMMAND ----------

input_data.where($"position"==="FI_ACCOUNT").show

// COMMAND ----------

input_data.show(100)

// COMMAND ----------


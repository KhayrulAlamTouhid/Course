// Databricks notebook source
//val num = sc.parallelize(List(1,2,3,4,5,6))

// COMMAND ----------

//check even and odd number using if else statement.
//val filter_num = num.map(x => if(x % 2==0) x + "number is even" else x + "number is odd").collect

// COMMAND ----------

val data_frame = sc.textFile("/FileStore/tables/data.csv")

// COMMAND ----------

val header = data_frame.first

// COMMAND ----------

val exect_data = data_frame.filter(x => (x != header))

// COMMAND ----------

val employee = exect_data.map(x => (x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3), x.split(",")(4), x.split(",")(5), x.split(",")(6))).toDF("id","first_name","last_name","country","number","position","SalAry")

// COMMAND ----------

//if data is so long than we have to use "table_name.show(false)"
// if we have to show limited columns than we have to use "table_name.show(10, false)"
employee.show(false)

// COMMAND ----------

//if i will show more prettyest way than i have to use this command.
display(employee)

// COMMAND ----------

//if i want to see dataframe scema than i have to write this command.
employee.printSchema
// if i want to see just column than i have to write "table_name.columns"

// COMMAND ----------

employee.select("id", "first_name","last_name", "country", "position").show

// COMMAND ----------

//Here i can use ".where" or ".filter" both are same thing but .where is DataFrame way and .filter is rdd way.
employee.select("id", "first_name", "country", "SalAry").where($"position" === "IT_PROG").show

// COMMAND ----------

employee.select("id", "first_name", "country", "SalAry").where($"position" === "IT_PROG" && $"SalAry" > 5000).show

// COMMAND ----------

import org.apache.spark.sql.functions._
// 5 Diffarents way I can defined column.
// "Column Name", col("Column Name"), column("Column Name"), $"Column Name", 'Column name 
//employee.select("id", col("first_name"), column("last_name"), $"country", 'SalAry) But i can't use defarent way in the same Dataframe. when i will use that i have to import this function.

// COMMAND ----------

// if we are to do column value is to uppercase than we have to use this statement.
employee.select($"id", upper($"first_name")).show(5)

// COMMAND ----------

//if we are to do column name is to uppcase than we have to use this statment.
employee.select("first_name".toUpperCase).show

// COMMAND ----------


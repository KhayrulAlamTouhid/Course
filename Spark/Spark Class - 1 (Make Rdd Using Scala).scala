// Databricks notebook source
val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9))

// COMMAND ----------

val sqrt_rdd = rdd.map(x => x*x).collect

// COMMAND ----------

sqrt_rdd.take(3)

// COMMAND ----------

val bignumber1 = sqrt_rdd.map(x => x > 50)
val bignumber2 = sqrt_rdd.filter(x => x > 50)

// COMMAND ----------

//Make new rdd on another way.
val my=sc.makeRDD( 1 to 20).collect
val my2= sc.range(1,7).collect

// COMMAND ----------

val rdd = sc.textFile("dbfs:/FileStore/tables/data-1.csv")


// COMMAND ----------

rdd.collect.foreach(println)

// COMMAND ----------

rdd.first

// COMMAND ----------

rdd.take(2)

// COMMAND ----------

val upper_rdd = rdd.map(x => x.toUpperCase)
upper_rdd.collect.foreach(println)

// COMMAND ----------

val rdd_lenth = upper_rdd.map(x => (x, x.length))
rdd_lenth.collect.foreach(println)

// COMMAND ----------

val sizeofrdd = rdd.filter(x => x.length > 55).collect

// COMMAND ----------

val rdd = sc.textFile("dbfs:/FileStore/tables/test.txt")

// COMMAND ----------

val split_key= rdd.flatMap(x => x.split(' '))

// COMMAND ----------

val to_upper = split_key.map(x => x.toUpperCase)


// COMMAND ----------

val key_valu = to_upper.map(x => (x,1))

// COMMAND ----------

val counting = key_valu.reduceByKey((a,b) => a+b)

// COMMAND ----------

counting.collect.foreach(println)

// COMMAND ----------

counting.filter(x => x._2 > 3).collect

// COMMAND ----------

counting.saveAsTextFile("/FileStore/tables/save.txt")

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables

// COMMAND ----------

val rdd5 = sc.textFile("/FileStore/tables/save.txt").collect.foreach(println)

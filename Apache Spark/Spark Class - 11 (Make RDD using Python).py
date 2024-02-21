# Databricks notebook source
"""
    1. Create Rdd using pyspark.
    2. Create Rdd to Testfile.
    3. Make word count programme using pyspark.
    4. Create DataFrame using pyspark.
    5. Use Group by with Agg functions
    6. Order by Column.
    7. Using Where functions.
"""

# COMMAND ----------

rdd = sc.parallelize(["sakib","tamim","musfiq","riyad","Hadoop"])

# COMMAND ----------

rdd.collect()

# COMMAND ----------

new_rdd = rdd.map(lambda x: x.upper())

# COMMAND ----------

new_rdd.collect()

# COMMAND ----------

lenthhh = new_rdd.map(lambda x: (x, len(x)))

# COMMAND ----------

conting = lenthhh.filter(lambda x: (x[1] > 5))

# COMMAND ----------

conting.collect()

# COMMAND ----------

# MAGIC %md ####Word Counting Programme using pyspark.

# COMMAND ----------

rdd = sc.textFile("dbfs:/FileStore/tables/test.txt")

# COMMAND ----------

rdd.collect()

# COMMAND ----------

# if i want to split one line then i have to write map function.
# or if i want split every line then i have to write flatMap funtion.
convert_rdd = rdd.flatMap(lambda x: x.split(" "))                       #value split.
sum_valu = convert_rdd.map(lambda x: (x , 1))                           #add value.
reduce_value = sums = sum_valu.reduceByKey(lambda x, y: x + y)          #reduce value.

# COMMAND ----------

filetr_rdd = reduce_value.filter(lambda x : (x[1] > 23))

# COMMAND ----------

filetr_rdd.collect()

# COMMAND ----------

# MAGIC %md ##### Create Dataframe using pyspark.

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/final_data.csv", header=True, inferSchema=True)
#Renamed column using pyspark.
renamecolumn = df.withColumnRenamed("_c0","Id").withColumnRenamed("_c2", "Dismisal")
#when we have to show full value in a column using "truncate= False"
renamecolumn.show(truncate=False)

# COMMAND ----------

df1 = spark.read.csv("/FileStore/tables/data.csv", header = True, inferSchema = True)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

group = df1.groupBy("position").agg(max("salary").alias("Maximum"), min("salary"))
orderbyasc = group.orderBy('Maximum', ascending=True)       #if i want to show Ascending way i have to use "ascending=True"
orderbydesc = group.orderBy('Maximum', ascending=False)     #if i want to show Descending way i have to use "ascending=False"

# COMMAND ----------

group.show(5,False)
orderbyasc.show(5, False)
orderbydesc.show(5, False)

# COMMAND ----------

#we can define column more way.
search = df1.where(col("position")== "IT_PROG")
search = df1.where(column("position")=="FI_ACCOUNT")
search = df1.where(df1.position == "AC_ACCOUNT")
# we can define more way.
# Databricks notebook source
"""
    1. Basic operation in python.
    2. How to download data from website using pyspark.
    3. Some operations of dbutils.
    4. Make dropdown, multiselect, and simple text box.
    5. Making graph, chart
    6. Create Simple Dashboard.
"""

# COMMAND ----------

course = "   #   Python programming    "

# COMMAND ----------

print(len(course))
print(course[1])
print(course[-4])
print(course[:])
print(course[1:5])

# COMMAND ----------

print(course.upper()) #Every value will be uppercase.
print(course.lower()) # Every value will be lowercase.
print(course.title()) # for Every value first world will be Capital Letter
print(course.strip()) #for remove every spach
print(course.lstrip()) #for remove left spaach.
print(course.rstrip()) # for remove Right spach, if i use rstrip("#") for remove special charecter in data.
print(course.find("P")) # for which charecter or value stay in which index
print(course.replace("Python","Apache Spark")) #for replace value
print("Python" in course) #writen bollean value.

# COMMAND ----------

# MAGIC %md #### How to download data from website using pyspark.

# COMMAND ----------

# first i have to import python libery urllib3
import urllib3

# COMMAND ----------

responce = urllib3.PoolManager().request('GET', "https://www.stats.govt.nz/assets/Uploads/Annual-balance-sheets/Annual-balance-sheets-2021-provisional/Download-data/annual-balance-sheets-2007-2021-provisional.csv")
csvfile = responce.data.decode("utf-8")
dbutils.fs.put("dbfs:/provissona.csv", csvfile)

# COMMAND ----------

mydata = spark.read.csv("dbfs:/provissona.csv", header=True)

# COMMAND ----------

mydata.show(10, False)

# COMMAND ----------

# MAGIC %md ####Some operation of dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# Check file path.
display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

# how to make text file.
dbutils.widgets.text("fname","Enter your first name:", "FirstName")
dbutils.widgets.text("lname","Enter your last name:", "Lastname")

# COMMAND ----------

#how can i call that.
getArgument("fname") + " " + getArgument("lname")

# COMMAND ----------

#if i want to remove all widget i have to run this command.

dbutils.widgets.removeAll()

# if i want to remove specefic one, i have to run this command.
#dbutils.widgets.remove('fname')

# COMMAND ----------

# MAGIC %md ####second Assisment

# COMMAND ----------

# make dropdown.
dbutils.widgets.dropdown("position","FI_ACCOUNT", ["FI_ACCOUNT", "SA_MAN", "PU_MAN", "ST_CLERK", "AD_ASST" ,"AD_VP"])

# COMMAND ----------

#make multiselect.
dbutils.widgets.multiselect("multiselect","FI_ACCOUNT", ["FI_ACCOUNT", "SA_MAN", "PU_MAN", "ST_CLERK", "AD_ASST" ,"AD_VP"])

# COMMAND ----------

#make textbox.
dbutils.widgets.text("check","list of position","position")

# COMMAND ----------

make_df = spark.read.csv("dbfs:/FileStore/tables/data-3.csv",header=True, inferSchema=True)

# COMMAND ----------

#make_df.where(make_df.salary >= getArgument("position")).show()

# COMMAND ----------

display(make_df.where(make_df.position == getArgument("position")))

# COMMAND ----------

make_df.where(col("position").isin(getArgument("multiselect"))).show()

# COMMAND ----------

make_df.orderBy(make_df.salary.desc()).show()

# COMMAND ----------

#make temporary view.
make_df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- how many distinct value are there
# MAGIC select distinct(df.position) from df;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- when i write sql or display built in function. then i will see markdown is top right corner.
# MAGIC select
# MAGIC   position,
# MAGIC   count(*) as member
# MAGIC from
# MAGIC   df
# MAGIC group by
# MAGIC   position
# MAGIC order by
# MAGIC   member desc
# MAGIC limit
# MAGIC   5;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT position, first_name, salary
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     position,
# MAGIC     first_name,
# MAGIC     salary,
# MAGIC     RANK() OVER (PARTITION BY position ORDER BY salary DESC) AS salary_rank
# MAGIC   FROM df
# MAGIC ) ranked_employee
# MAGIC WHERE salary_rank = 1 LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df limit 5;

# COMMAND ----------


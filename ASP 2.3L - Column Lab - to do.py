# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Columns Lab
# MAGIC
# MAGIC Prepare final dataset based on tasks from table parties
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Add new column
# MAGIC 2. Filter parties where PT_TITLE_BEFORE is not null
# MAGIC 3. Check what types of origin we have
# MAGIC 4. Drop unneeded column
# MAGIC
# MAGIC ##### Methods
# MAGIC - DataFrame: **`select`**, **`drop`**, **`withColumn`**, **`filter`**, **`dropDuplicates`**
# MAGIC - Column: **`isNotNull`**, **`like`**

# COMMAND ----------

parties_df = spark.read.table("cip_catalog.cleansed_dwh_owner.parties")
display(parties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Add new column
# MAGIC ### Add new column **`total_key`** by sum of columns **`cntry_born_key`** and **`cntry_citizen_key`**

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

total_df = parties_df.#FILL_IN
display(total_df.select("cntry_born_key","cntry_citizen_key", "total_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Filter total_df where PT_TITLE_BEFORE is not null
# MAGIC Filter for records where **`PT_TITLE_BEFORE`** is not **`null`**

# COMMAND ----------

title_df = total_df.#TO DO

display(title_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Check what types of origins we have
# MAGIC Find unique **`PTORI_KEY`** values in **`title_df`** in one of two ways:
# MAGIC - Select "PTORI_KEY" and get distinct records
# MAGIC - Drop duplicate records
# MAGIC

# COMMAND ----------

# Method 1
distinct_df1 = total_df.#FILL_IN
display(distinct_df1)


# COMMAND ----------

# Method 2
distinct_df2 = total_df.#FILL_IN
display(distinct_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Drop unneeded column
# MAGIC Since there's no need of column **'total_key'**, drop it from **`title_df`**.

# COMMAND ----------

final_df = title_df.#FILL_IN
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 5. Chain all the steps above excluding step 3

# COMMAND ----------

final_df_again = (parties_df
            #FILL_IN
           )

display(final_df_again)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>

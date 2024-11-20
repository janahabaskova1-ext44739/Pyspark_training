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
# MAGIC # New users
# MAGIC Get new user which have never been client
# MAGIC 1. Get users
# MAGIC 2. Get new users
# MAGIC 3. Join them
# MAGIC 4. Filter for new users which are not old clients
# MAGIC
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cells below to create DataFrames **`users_df`**, and **`new_users_df`**.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# Read the parties table
users_df = spark.read.table("cip_catalog.cleansed_dwh_owner.parties")

# COMMAND ----------

new_users_df=spark.read.table("cip_catalog.cleansed_dami_owner.pt_uni_lookup")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1: Get birth number of clients
# MAGIC - Select the **`PT_BIRTH_NUMBER_CODE`** column in **`users_df`** and remove duplicates
# MAGIC - Add a new column **`checked`** with the boolean **`True`** for all rows
# MAGIC
# MAGIC Save the result as **`checked_users_df`**.

# COMMAND ----------

checked_users_df = (users_df
                      .#FILL_IN
                     )
display(checked_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2: Join checked users with new users
# MAGIC - Perform an outer join on **`checked_users_df`** and **`new_users_df`** with the **`PT_BIRTH_NUMBER_CODE`** field
# MAGIC - Fill null values in **`checked`** as **`False`**
# MAGIC
# MAGIC Save the result as **`all_users_df`**.

# COMMAND ----------

all_users_df = (new_users_df
                  .#FILL_IN
                 )
display(all_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3: Filter for new users with name
# MAGIC - Filter **`all_users_df`** for users where **`checked`** is False
# MAGIC - Filter for users with non-null last name
# MAGIC
# MAGIC Save result as **`non_checked_users_df`**.

# COMMAND ----------

non_checked_users_df = (all_users_df
                    .#FILL_IN
                     )
display(non_checked_users_df)

# COMMAND ----------

# MAGIC %md
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

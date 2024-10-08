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
# MAGIC # Beginning of user's business activites Lab
# MAGIC Plot daily beginning of user's business activites and average users's activation by day of week.
# MAGIC 1. Extract date of beginning of business activities
# MAGIC 2. Get daily business activation
# MAGIC 3. Get average number of activation by day of week
# MAGIC 4. Sort day of week in correct order

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Read the parties table
parties_df = spark.read.table("cip_catalog.cleansed_dwh_owner.parties").select('PT_KEY','PT_start_of_business','PTORI_KEY').where(col('PTORI_KEY')==4)

display(parties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Extract date of starting business
# MAGIC - Add **`SOB_date`** column by converting **`PT_start_of_business`** to date

# COMMAND ----------

from pyspark.sql.functions import to_date

start_business_date_df = (parties_df
               .#FILL IN
              )
display(start_business_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Get daily user's business starting days
# MAGIC - Group by SOB_date
# MAGIC - Aggregate approximate count of distinct **`PT_KEY`** and alias to "users_business_start"
# MAGIC   - Recall built-in function to get **approximate count distinct** (also recall:  approximate count distinct is different than count distinct!)
# MAGIC - Sort by SOB_date
# MAGIC - Plot as line graph

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct

active_business_df = (start_business_date_df
                   #FILL IN
                  )
display(active_business_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Get average number of activations of business activities by day of week
# MAGIC - Add **`day`** column by extracting day of week from **`SOB_date`** using a datetime pattern string - the expected output here will be a day name, not a number (e.g. **`Mon`**, not **`1`**)
# MAGIC - Group by **`day`**
# MAGIC - Aggregate average of **`users_business_start`** and alias to "avg_users_business_start"

# COMMAND ----------

from pyspark.sql.functions import date_format, avg

active_doy_df = (active_business_df
                 #FILL IN
                )
display(active_doy_df)

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

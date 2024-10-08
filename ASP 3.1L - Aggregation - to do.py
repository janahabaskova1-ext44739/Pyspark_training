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
# MAGIC # Clients score from Credit Bureau by Country
# MAGIC Get the 3 country code generating the highest score from Credit Bureau.
# MAGIC 1. Aggregate revenue by country code
# MAGIC 2. Get top 3 country code by score from credit bureau
# MAGIC 3. Clean credit columns to have two decimal places
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**, **`sort`**, **`limit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`desc`**, **`cast`**, **`operators`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-in Functions</a>: **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cell below to create the starting DataFrame **`parties_df`**.

# COMMAND ----------

# Read the parties table
parties_df = spark.read.table("cip_catalog.cleansed_dwh_owner.parties")

display(parties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Aggregate bureau score by country 
# MAGIC - Group by **`cntry_resident_key`**
# MAGIC - Get sum of **`PT_BUREAU_SCORE`** as **`total_bureau_score`**. 
# MAGIC - Get average of **`PT_BUREAU_SCORE`** as **`avg_bureau_score`**
# MAGIC
# MAGIC Remember to import any necessary built-in functions.

# COMMAND ----------

from pyspark.sql.functions import avg, col, sum

# COMMAND ----------

residence_score_df = (parties_df
              .#FILL_IN
              .#FILL_IN
             )

display(residence_score_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Get top three countries by total bureau score
# MAGIC - Sort by **`total_bureau_score`** in descending order
# MAGIC - Limit to first three rows

# COMMAND ----------

top_residence_score_df = residence_score_df.#FILL_IN
display(top_residence_score_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Limit avg_bureau_score column to two decimal places
# MAGIC - Modify columns **`avg_bureau_score`** to contain numbers with two decimal places
# MAGIC   - Use **`withColumn()`** with the same names to replace these columns
# MAGIC   - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100

# COMMAND ----------

final_residence_score_df = (top_residence_score_df
            .#FILL_IN
           )

display(final_residence_score_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Bonus: Rewrite using a built-in math function
# MAGIC Find a built-in math function that rounds to a specified number of decimal places

# COMMAND ----------

from pyspark.sql.functions import round

# COMMAND ----------


bonus_df = (top_residence_score_df
            .#FILL_IN
           )

display(bonus_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 5. Chain all the steps above

# COMMAND ----------

# Solution #1 using round

chain_df = (parties_df
            .#FILL_IN
           )

display(chain_df)

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

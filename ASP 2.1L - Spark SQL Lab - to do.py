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
# MAGIC # Spark SQL Lab
# MAGIC
# MAGIC ##### Tasks
# MAGIC 1. Create a DataFrame from the **`parties`** table
# MAGIC 1. Display the DataFrame and inspect its schema
# MAGIC 1. Apply transformations to filter and sort clients from **`CRM`**
# MAGIC 1. Count results and take the first 5 rows
# MAGIC 1. Create the same DataFrame using a SQL query
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations: **`select`**, **`where`**, **`orderBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> actions: **`select`**, **`count`**, **`take`**
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 1. Create a DataFrame from the **`parties`** table
# MAGIC - Use SparkSession to create a DataFrame from the **`parties`** table

# COMMAND ----------

parties_df = #FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Display DataFrame and inspect schema
# MAGIC - Use methods above to inspect DataFrame contents and schema

# COMMAND ----------

#FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 3. Apply transformations to filter and sort clients from **`CRM`**
# MAGIC - Filter for rows where **`PTORI_KEY`** is **`4`**
# MAGIC - Sort rows by **`Date of account openning`**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> Use single and double quotes in your filter SQL expression

# COMMAND ----------

crm_df = (parties_df
          #FILL_IN
         )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 4. Count results and take first 5 rows
# MAGIC - Use DataFrame actions to count and take rows

# COMMAND ----------

num_rows = crm_df.#FILL_IN
rows = crm_df.#FILL_IN

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 5. Create the same DataFrame using SQL query
# MAGIC - Use SparkSession to run a SQL query on the **`parties`** table
# MAGIC - Use SQL commands to write the same filter and sort query used earlier

# COMMAND ----------

crm_sql_df = spark.#FILL_IN

display(crm_sql_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Classroom Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>

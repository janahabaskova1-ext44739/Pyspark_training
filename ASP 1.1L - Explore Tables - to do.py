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
# MAGIC ### 1. Look on table from DWH
# MAGIC
# MAGIC select all from table _cip_catalog.cleansed_dwh_owner.parties_

# COMMAND ----------


# TO DO

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 2. Explore datasets table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED cip_catalog.cleansed_dwh_owner.parties

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 2.1: What is the average number (on 2 decimal places) of Employees (PT_NUMBER_OF_EMPLOYEES)?
# MAGIC
# MAGIC
# MAGIC Execute a SQL query that computes the average **`PT_NUMBER_OF_EMPLOYEES`** from the **`cip_catalog.cleansed_dwh_owner.parties`** table.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> The result should be **`11.19`**.

# COMMAND ----------

# TO DO

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### 2.2: What priority groups we have?
# MAGIC
# MAGIC
# MAGIC Execute a SQL query that computes the distinct values of PT_PRIORITY_GROUPS from the cip_catalog.cleansed_dwh_owner.parties table.
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> You should see **`19`** distinct values.

# COMMAND ----------

# TO DO

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

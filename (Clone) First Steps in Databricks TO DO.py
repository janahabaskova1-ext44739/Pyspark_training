# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Databricks Platform
# MAGIC
# MAGIC První kroky v Databricksu.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Který programovací jazyk je tvůj oblíbený?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Magic příkazy pro volbu programovacího jazyka: **`%python`**, **`%scala`**, **`%sql`**, **`%r`**

# COMMAND ----------

# MAGIC %md
# MAGIC Python

# COMMAND ----------

print("Run python")

# COMMAND ----------

# MAGIC %md
# MAGIC Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Run scala")

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Run SQL"

# COMMAND ----------

# MAGIC %md
# MAGIC R

# COMMAND ----------

# MAGIC %r
# MAGIC print("Run R", quote=FALSE)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Kde se berou ty buňky s textem?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Nadpis 1
# MAGIC ## Podnadpis
# MAGIC ### Podnadpis 2
# MAGIC
# MAGIC
# MAGIC 1. **tučně**
# MAGIC 2. *kurzíva*
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Načteme si data
# MAGIC

# COMMAND ----------

# načti data z tabulky "cip_catalog.cleansed_dwh_owner.parties" 

df_parties = # To do


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- načti data z tabulky "cip_catalog.cleansed_dwh_owner.parties" 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Připravme si data na vizualiaci

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Popis strukturu tabulky 'cip_catalog.cleansed_dwh_owner' 
# MAGIC -- TO DO

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 
# MAGIC -- Select birth date and count the number of people born on each date
# MAGIC SELECT PT_BIRTH_DATE AS birth_date,
# MAGIC        COUNT(*) AS number_of_people
# MAGIC FROM cip_catalog.cleansed_dwh_owner.parties
# MAGIC WHERE YEAR(PT_BIRTH_DATE) >= 1920
# MAGIC GROUP BY PT_BIRTH_DATE
# MAGIC ORDER BY number_of_people DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Přidáme parametr pomocí widgetu
# MAGIC

# COMMAND ----------

dbutils.widgets.text("csas_employee", "", "PT_JOB_STAFF_FLAG")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Obohať předchozí kód pomocí widgetu **`getArgument`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select birth date and count the number of people born on each date
# MAGIC SELECT PT_BIRTH_DATE AS birth_date,
# MAGIC        COUNT(*) AS number_of_people
# MAGIC FROM cip_catalog.cleansed_dwh_owner.parties
# MAGIC WHERE YEAR(PT_BIRTH_DATE) >= 1920 AND PT_JOB_STAFF_FLAG = getArgument("csas_employee")
# MAGIC GROUP BY PT_BIRTH_DATE
# MAGIC ORDER BY number_of_people DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Odstraň widget

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET csas_employee
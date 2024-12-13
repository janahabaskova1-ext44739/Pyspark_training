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
# MAGIC- [markdown-documentation](https://www.markdownguide.org/cheat-sheet/) 


# MAGIC| Element         | Markdown Syntax |
# MAGIC|-----------------|-----------------|
# MAGIC| Heading         | `#H1` `##H2` `###H3` `#### H4` `##### H5` `###### H6` |
# MAGIC| Block quote     | `> blockquote` |
# MAGIC| Bold            | `**bold**` |
# MAGIC| Italic          | `*italicized*` |
# MAGIC| Strikethrough   | `~~strikethrough~~` |
# MAGIC| Horizontal Rule | `---` |
# MAGIC| Code            | ``` `code` ``` |
# MAGIC| Link            | `[text](https://www.example.com)` |
# MAGIC| Image           | `![alt text](image.jpg)`|
# MAGIC| Ordered List    | `1. First items` <br> `2. Second Item` <br> `3. Third Item` |
# MAGIC| Unordered List  | `- First items` <br> `- Second Item` <br> `- Third Item` |
# MAGIC| Code Block      | ```` ``` ```` <br> `code block` <br> ```` ``` ````|
# MAGIC| Table           |<code> &#124; col &#124; col &#124; col &#124; </code> <br> <code> &#124;---&#124;---&#124;---&#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br>|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Načteme si data
# MAGIC

# COMMAND ----------

# načti data z tabulky titanic ("prod_cze_peal00_lab_catalog.e2e_workshop.titanic") a zobraz data

df_titanic = # To do


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- načti data z tabulky titanic ("prod_cze_peal00_lab_catalog.e2e_workshop.titanic") pomocí SQL
# MAGIC -- TO DO
# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Připravme si data na vizualiaci

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Popis strukturu tabulky titanic ("prod_cze_peal00_lab_catalog.e2e_workshop.titanic") 
# MAGIC -- TO DO

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Rozdel data podle tridy a mista nalodeni a vytvor jednoduchy graf.
# MAGIC -- TO DO

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Přidáme parametr pomocí widgetu
# MAGIC

# COMMAND ----------

dbutils.widgets.text("Pohlaví", "female")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Obohať předchozí kód pomocí widgetu **`getArgument`**

# COMMAND ----------

# MAGIC ##TO DO


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Odstraň widget

# COMMAND ----------

# MAGIC dbutils.widgets.remove("Pohlaví")

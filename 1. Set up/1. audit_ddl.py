# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS tp_hc_adb_ws.audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tp_hc_adb_ws.audit.load_logs (
# MAGIC     data_source STRING,
# MAGIC     tablename STRING,
# MAGIC     numberofrowscopied INT,
# MAGIC     watermarkcolumnname STRING,
# MAGIC     loaddate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table  tp_hc_adb_ws.audit.load_logs 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from audit.load_logs

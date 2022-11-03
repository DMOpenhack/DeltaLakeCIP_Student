-- Databricks notebook source
-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS default.loan_risks_upload;
-- MAGIC 
-- MAGIC CREATE TABLE default.loan_risks_upload (
-- MAGIC   loan_id BIGINT,
-- MAGIC   funded_amnt INT,
-- MAGIC   paid_amnt DOUBLE,
-- MAGIC   addr_state STRING
-- MAGIC ) USING DELTA LOCATION '/mnt/myDeltaLake';
-- MAGIC 
-- MAGIC COPY INTO default.loan_risks_upload
-- MAGIC FROM '/databricks-datasets/learning-spark-v2/loans/loan-risks.snappy.parquet'
-- MAGIC FILEFORMAT = PARQUET;
-- MAGIC 
-- MAGIC SELECT * FROM default.loan_risks_upload;

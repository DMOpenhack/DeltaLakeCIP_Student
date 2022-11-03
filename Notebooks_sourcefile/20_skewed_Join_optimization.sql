-- Databricks notebook source
-- MAGIC %sql
-- MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
-- MAGIC CREATE or replace TEMPORARY VIEW orders_csv
-- MAGIC USING CSV
-- MAGIC OPTIONS (path "/mnt/deltalake/skew/orders.csv", header "true", mode "FAILFAST");
-- MAGIC 
-- MAGIC CREATE or replace TEMPORARY VIEW city_csv
-- MAGIC USING CSV
-- MAGIC OPTIONS (path "/mnt/deltalake/skew/city.csv", header "true", mode "FAILFAST");

-- COMMAND ----------

Create or replace table db1.orders
location '/mnt/myDeltaLake/skew_Delta/'
as
Select * from orders_csv

-- COMMAND ----------

Create or replace table db1.city
location '/mnt/myDeltaLake/skew_Delta_city/'
as
Select * from city_csv

-- COMMAND ----------

Describe table db1.city

-- COMMAND ----------

optimize db1.orders

-- COMMAND ----------

SELECT  c.city,Avg(quantity),count(*) as count_rows FROM db1.city c, db1.orders o WHERE o.city_id = c.city_id  
Group by c.city

-- COMMAND ----------

SELECT  /*+ SKEW('o','city_id',1) */ c.city,Avg(quantity), count(*) as count_rows FROM db1.city c, db1.orders o WHERE o.city_id = c.city_id 
Group by c.city

-- COMMAND ----------

SELECT  /*+ SKEW('o','city_id',1) */ o.id,c.city,o.quantity as count_rows FROM db1.city c, db1.orders o WHERE o.city_id = c.city_id and c.city_id= 2


-- COMMAND ----------

SELECT o.id,c.city,o.quantity as count_rows FROM db1.city c, db1.orders o WHERE o.city_id = c.city_id and c.city_id = 2

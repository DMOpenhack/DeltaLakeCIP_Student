# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT SELECT ON db1.diamonds_sql to `arr.nagaraj@gmail.com`

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table db1.users(user_id int, group_name varchar(40));
# MAGIC Insert into db1.users select 1, 'admins';
# MAGIC Insert into db1.users select 2, 'customer';
# MAGIC Insert into db1.users select 3, 'employee';
# MAGIC 
# MAGIC Create or replace table db1.orders(order_id int,user_id int, transact_date date, amount int);
# MAGIC Insert into db1.orders select 100,1,'2021-01-03',3500;
# MAGIC Insert into db1.orders select 150,1,'2021-01-04',5500;
# MAGIC Insert into db1.orders select 103,2,'2021-01-04',5000;
# MAGIC Insert into db1.orders select 120,3,'2021-01-02',2500;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace VIEW db1.orders_rls
# MAGIC as
# MAGIC 
# MAGIC Select order_id, transact_date, amount
# MAGIC from db1.orders o inner join db1.users u on o.user_id = u.user_id 
# MAGIC where is_member(u.group_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db1.orders_rls

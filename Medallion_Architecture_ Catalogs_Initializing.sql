-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Creating Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS bank_catalog;

USE CATALOG bank_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Creating Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS  bronze_schema;
CREATE SCHEMA IF NOT EXISTS  silver_schema;
CREATE SCHEMA IF NOT EXISTS  gold_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_schema.customers_raw(
  customer_id STRING,
  name STRING,
  email STRING,
  phone STRING,
  address STRING,
  credit_score INT,
  join_date DATE,
  last_update TIMESTAMP
)

-- COMMAND ----------

DESCRIBE FORMATTED bronze_schema.customers_raw;

-- COMMAND ----------

select * from bronze_schema.customers_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Branch Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_schema.branches_raw(
  branch_id STRING,
  branch_name STRING,
  location STRING,
  timezone STRING
)

-- COMMAND ----------

DESCRIBE FORMATTED bronze_schema.branches_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transaction Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_schema.transactions_raw(
    transaction_id STRING,
    customer_id STRING,
    branch_id STRING,
    channel STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    timestamp STRING,
    status STRING
)


-- COMMAND ----------

DESCRIBE FORMATTED bronze_schema.transactions_raw;

-- COMMAND ----------

select * from bronze_schema.transactions_raw;

-- COMMAND ----------



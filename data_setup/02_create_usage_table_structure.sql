-- ================================================================
-- CCA BigQuery Analytics: Daily Usage Facts Table Structure
-- Creates the partitioned and clustered table structure for usage data
-- File: data_setup/02_create_usage_table_structure.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-19
-- ================================================================

CREATE OR REPLACE TABLE `cca-bigquery-analytics.cca_demo.daily_usage_facts` (
  usage_date DATE NOT NULL,
  customer_id STRING NOT NULL,
  kwh_used FLOAT64,
  peak_demand_kw FLOAT64,
  cost_dollars FLOAT64,
  rate_plan STRING,
  customer_type STRING,
  city STRING,
  temperature_high_f INT64,
  is_weekend BOOL,
  is_holiday BOOL,
  PRIMARY KEY (usage_date, customer_id) NOT ENFORCED, -- composite key ensuring one record per customer per day
  FOREIGN KEY (customer_id) REFERENCES `cca-bigquery-analytics.cca_demo.customers`(customer_id) NOT ENFORCED
)
PARTITION BY usage_date
CLUSTER BY customer_type, city
OPTIONS(
  description="Daily energy usage facts with realistic consumption patterns - Primary Key: (usage_date, customer_id), Foreign Key: customer_id -> customers.customer_id",
  partition_expiration_days=2555  -- ~7 years retention
);
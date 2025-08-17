-- ================================================================
-- City Usage Summary Data Mart
-- Aggregates customer usage patterns by geography and customer type
-- dbt model: models/city_usage_summary.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-16
-- ================================================================

{{ config(materialized='table') }}

SELECT 
  city,
  customer_type,
  COUNT(*) as customer_count,
  ROUND(AVG(avg_daily_kwh), 2) as avg_city_usage,
  ROUND(AVG(total_cost_ytd),2) as avg_annual_cost,
  ROUND(AVG(effective_rate_per_kwh),2) as avg_rate_per_kwh
FROM `cca-bigquery-analytics.cca_demo.customer_rankings_results`
GROUP BY city, customer_type
ORDER BY avg_city_usage DESC
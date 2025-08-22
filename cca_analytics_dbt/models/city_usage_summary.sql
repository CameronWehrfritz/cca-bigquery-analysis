-- ================================================================
-- City Usage Summary Data Mart
-- Aggregates customer usage patterns by geography and customer type
-- dbt model: models/city_usage_summary.sql
-- Author: Cameron Wehrfritz
-- Created: 2025-08-16
-- Updated: 2025-08-22 - Updated to reference customer_rankings dbt model
-- ================================================================

{{ config(materialized='table') }}

SELECT
  city,
  customer_type,
  COUNT(*) as customer_count,
  ROUND(AVG(avg_daily_kwh), 2) as avg_city_usage_kwh,
  ROUND(AVG(total_cost_ytd), 2) as avg_annual_cost,
  ROUND(AVG(effective_dollars_per_kwh), 2) as avg_dollars_per_kwh
FROM {{ ref('customer_rankings') }}
GROUP BY city, customer_type
ORDER BY avg_city_usage_kwh DESC
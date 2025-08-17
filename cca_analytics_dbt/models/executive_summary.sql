-- ================================================================
-- Executive Summary Data Mart
-- High-level metrics aggregated from city usage patterns
-- dbt model: models/executive_summary.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-16
-- ================================================================

{{ config(materialized='view') }}

SELECT
  COUNT(DISTINCT city) as total_cities,
  COUNT(DISTINCT customer_type) as customer_segments,
  SUM(customer_count) as total_customers,
  ROUND(AVG(avg_city_usage_kwh), 2) as overall_avg_usage_kwh,
  ROUND(MAX(avg_city_usage_kwh), 2) as peak_usage_segment_kwh,
  ROUND(MIN(avg_city_usage_kwh), 2) as lowest_usage_segment_kwh
FROM {{ ref('city_usage_summary') }}
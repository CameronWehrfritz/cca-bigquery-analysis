-- ================================================================
-- CCA Customer Segmentation & Basic Exploration
-- Demonstrates BigQuery optimization and CCA domain knowledge
-- ================================================================

-- Query 1: Customer Overview with BigQuery Optimization
-- SELECT
--   customer_type,
--   city,
--   rate_plan,
--   COUNT(*) as customer_count,
--   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
--   -- Show adoption rates
--   ROUND(AVG(CASE WHEN has_solar THEN 1.0 ELSE 0.0 END) * 100, 1) as solar_adoption_pct,
--   ROUND(AVG(CASE WHEN has_ev THEN 1.0 ELSE 0.0 END) * 100, 1) as ev_adoption_pct,
--   ROUND(AVG(CASE WHEN has_battery_storage THEN 1.0 ELSE 0.0 END) * 100, 1) as battery_adoption_pct,
--   ROUND(AVG(CASE WHEN is_low_income_qualified THEN 1.0 ELSE 0.0 END) * 100, 1) as low_income_pct
-- FROM `cca-bigquery-analytics.cca_demo.customers`
-- GROUP BY customer_type, city, rate_plan
-- ORDER BY customer_count DESC;

-- -- Query 2: Usage Patterns by Customer Segment (with partition pruning!)
-- SELECT 
--   customer_type,
--   city,
--   COUNT(DISTINCT customer_id) as active_customers,
--   ROUND(AVG(kwh_used), 2) as avg_daily_kwh,
--   ROUND(SUM(kwh_used), 0) as total_kwh,
--   ROUND(SUM(cost_dollars), 0) as total_revenue,
--   ROUND(AVG(cost_dollars), 2) as avg_daily_cost
-- FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
-- WHERE usage_date >= '2024-07-01'  -- Only July 2024 (partition pruning!)
--   AND usage_date < '2024-08-01'
-- GROUP BY customer_type, city
-- ORDER BY total_kwh DESC;

-- Query 3: Rate Plan Performance Analysis
-- This query benefits from clustering since we GROUP BY rate_plan,
-- and the table is clustered by customer_type, city (related groupings)
SELECT
  rate_plan,
  COUNT(DISTINCT customer_id) as customers,
  ROUND(AVG(kwh_used), 2) as avg_daily_usage,
  ROUND(AVG(cost_dollars), 2) as avg_daily_revenue,
  ROUND(SUM(cost_dollars) / SUM(kwh_used), 4) as effective_rate_per_kwh,
  COUNT(*) as total_records  -- Total rows in each rate plan group
FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
WHERE usage_date >= '2024-01-01'
GROUP BY rate_plan
ORDER BY customers DESC;
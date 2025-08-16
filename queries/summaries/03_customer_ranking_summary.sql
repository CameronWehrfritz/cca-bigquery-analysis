-- ================================================================
-- Customer Ranking Analysis - Summary for README
-- Key business insights aggregated for markdown table
-- File: queries/summaries/03_customer_ranking_summary.sql
-- Author: Cameron Wehrfritz
-- Created: 2024-08-15
-- ================================================================

-- ================================================================
-- Customer Ranking Analysis - Summary for README
-- Key business insights aggregated for markdown table
-- ================================================================

WITH customer_summary AS (
  -- Requires: Run 03_customer_ranking_analysis.sql and save results to "customer_rankings_results" table
  SELECT *
  FROM `cca-bigquery-analytics.cca_demo.customer_rankings_results`
)

-- Summary 1: Value Segment Distribution
SELECT 
  'Value Segment Distribution' as metric_category,
  usage_segment as segment,
  COUNT(*) as customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(total_kwh_ytd), 0) as avg_usage_kwh,
  ROUND(AVG(total_cost_ytd), 0) as avg_revenue
FROM customer_summary
GROUP BY usage_segment

UNION ALL

-- Summary 2: Technology Adoption Patterns  
SELECT 
  'Technology Adoption' as metric_category,
  CASE 
    WHEN has_solar AND has_ev AND has_battery_storage THEN 'Full Tech Adopter'
    WHEN has_solar AND has_ev THEN 'Solar + EV'
    WHEN has_solar AND has_battery_storage THEN 'Solar + Storage'
    WHEN has_ev AND has_battery_storage THEN 'EV + Storage'
    WHEN has_solar THEN 'Solar Only'
    WHEN has_ev THEN 'EV Only'
    WHEN has_battery_storage THEN 'Storage Only'
    ELSE 'No Tech'
  END as segment,
  COUNT(*) as customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(total_kwh_ytd), 0) as avg_usage_kwh,
  ROUND(AVG(total_cost_ytd), 0) as avg_revenue
FROM customer_summary
GROUP BY has_solar, has_ev, has_battery_storage

UNION ALL

-- Summary 3: Risk Assessment Distribution
SELECT 
  'Risk Assessment' as metric_category,
  risk_indicator as segment,
  COUNT(*) as customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(total_kwh_ytd), 0) as avg_usage_kwh,
  ROUND(AVG(total_cost_ytd), 0) as avg_revenue
FROM customer_summary
GROUP BY risk_indicator

UNION ALL

-- Summary 4: Top Customer Segments by Value
SELECT 
  'Top Performers' as metric_category,
  CONCAT(customer_type, ' - Top 10%') as segment,
  COUNT(*) as customer_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
  ROUND(AVG(total_kwh_ytd), 0) as avg_usage_kwh,
  ROUND(AVG(total_cost_ytd), 0) as avg_revenue
FROM customer_summary
WHERE usage_percentile_in_segment >= 90
GROUP BY customer_type

ORDER BY metric_category, customer_count DESC;
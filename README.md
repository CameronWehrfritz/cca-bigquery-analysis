# CCA BigQuery Analytics

## About CCAs
Community Choice Aggregators (CCAs) are public agencies that provide electricity to customers within their jurisdictions, typically focusing on renewable energy and community benefits.

## Project Overview

This project demonstrates enterprise-level data analytics for CCA utility operations using Google BigQuery. The analysis portfolio showcases advanced SQL techniques applied to synthetic energy usage data, providing actionable insights for utility operations, customer management, and regulatory compliance.

## Dataset Description

The synthetic dataset models a realistic CCA serving Peninsula Clean Energy's territory with:

- **39.59 million daily usage records** spanning January 2022 to August 2024
- **50,000 customers** across residential, small commercial, and large commercial segments
- **Customer demographics** including solar adoption, EV ownership, battery storage, and low-income qualifications
- **Program enrollments** tracking participation in clean energy incentive programs
- **Rate plans** reflecting actual CCA pricing structures (ECO, ECO100, ECOplus)

## Technical Architecture

**Platform:** Google BigQuery

**Data Volume:** 39.59M records across 3 tables

**Storage Strategy:** Date-partitioned tables optimized for time-series queries

**Query Complexity:** Advanced analytics with multi-table JOINs and window functions

## Analytics Portfolio

### 1. Monthly Trends Analysis (`01_monthly_trends_analysis.sql`)
- **Purpose:** Year-over-year and month-over-month growth analysis
- **Techniques:** LAG window functions, CTEs, seasonal pattern detection
- **Business Value:** Capacity planning, regulatory reporting, growth forecasting
- **Key Finding:** Consistent 30-50% YoY growth across all customer segments

### 2. Seasonal Pattern Analysis (`02_seasonal_analysis.sql`)
- **Purpose:** Advanced time-series analysis with volatility smoothing
- **Techniques:** Moving averages, conditional aggregations, variance calculations
- **Business Value:** Demand response planning, weather correlation, anomaly detection
- **Key Finding:** Clear seasonal patterns with winter usage peaks and summer efficiency valleys

### 3. Customer Ranking Analysis (`03_customer_ranking_analysis.sql`)
- **Purpose:** Multi-dimensional customer segmentation and risk assessment
- **Techniques:** Multiple window function types, percentile analysis, dynamic thresholds
- **Business Value:** Account management, retention programs, targeted marketing
- **Key Finding:** Automated identification of high-value customers and churn risks

### 4. Advanced Multi-Table Analysis (`04_advanced_multi_table_analysis.sql`)
- **Purpose:** Comprehensive customer lifecycle and program effectiveness measurement
- **Techniques:** Complex JOINs, before/after analysis, city benchmarking, technology profiling
- **Business Value:** Program ROI measurement, geographic performance analysis, opportunity identification
- **Key Finding:** Quantified program impacts showing 8-12% usage reductions post-enrollment

## Technical Highlights

**Advanced SQL Patterns:**
- Common Table Expressions (CTEs) for complex data organization
- Window functions (LAG, LEAD, ROW_NUMBER, PERCENT_RANK, NTILE, PERCENTILE_CONT)
- Multi-table JOINs with fact and dimension tables
- Conditional aggregations for segment-specific analysis
- Null-safe calculations and error handling

**Performance Optimization:**
- Partition-aware date filtering for query efficiency
- Proper use of clustering and partitioning strategies
- Minimal data scanning through targeted WHERE clauses
- Efficient aggregation patterns for large datasets

**Data Quality Management:**
- Comprehensive null handling with NULLIF and COALESCE
- Edge case management for division operations
- Data consistency validation across time periods
- Proper handling of customer lifecycle changes

## Business Applications

**Utility Operations:**
- Load forecasting and capacity planning
- Peak demand management and grid stability
- Renewable energy integration planning
- Infrastructure investment prioritization

**Customer Management:**
- Churn prediction and retention strategies
- Personalized program recommendations
- Account management prioritization
- Customer value segmentation

**Regulatory Compliance:**
- Growth reporting and trend analysis
- Program effectiveness measurement
- Service territory performance monitoring
- Rate impact assessment

## Getting Started

### Prerequisites
- Google Cloud Platform account with BigQuery access
- BigQuery command-line tool (`bq`) installed
- **Dataset Generation:** Run `/data/synthetic_data_creation.sql` to create the CCA synthetic dataset (39.59M records across 3 tables)

### Running the Queries
```bash
# Navigate to project directory
cd cca-bigquery-analysis

# Run individual queries
bq query --use_legacy_sql=false --max_rows=1000 < queries/03_customer_ranking_analysis.sql

# Export results to CSV  
bq query --use_legacy_sql=false --format=csv --max_rows=5000 < queries/03_customer_ranking_analysis.sql > customer_rankings.csv

# Save results for summary analysis
bq query --use_legacy_sql=false --destination_table=cca_demo.customer_rankings_results < queries/03_customer_ranking_analysis.sql
bq query --use_legacy_sql=false < queries/summaries/03_customer_ranking_summary.sql
```

### File Structure
```
cca-bigquery-analysis/
├── README.md
├── queries/
│   ├── 01_monthly_trends_analysis.sql
│   ├── 02_seasonal_analysis.sql
│   ├── 03_customer_ranking_analysis.sql
│   └── 04_advanced_multi_table_analysis.sql
└── data/
    └──  synthetic_data_creation.sql
```

## Sample Results: Customer Ranking Analysis

### Technology Adoption Distribution
| Technology Profile | Customer Count | Percentage | Avg Usage (kWh) | Avg Revenue ($) |
|-------------------|----------------|------------|-----------------|-----------------|
| No Tech | 34,414 | 68.8% | 15,086 | 3,072 |
| EV Only | 6,115 | 12.2% | 17,949 | 3,659 |
| Solar Only | 4,602 | 9.2% | 13,524 | 2,759 |
| Storage Only | 3,023 | 6.0% | 15,713 | 3,212 |
| Solar + EV | 828 | 1.7% | 15,052 | 3,064 |
| EV + Storage | 558 | 1.1% | 17,487 | 3,575 |
| Solar + Storage | 387 | 0.8% | 14,372 | 2,905 |
| Full Tech Adopter | 73 | 0.1% | 15,385 | 3,086 |

### Value Segment Distribution
| Usage Segment | Customer Count | Percentage | Avg Usage (kWh) | Avg Revenue ($) |
|---------------|----------------|------------|-----------------|-----------------|
| Average | 20,001 | 40.0% | 15,136 | 3,083 |
| Above Average | 9,999 | 20.0% | 15,778 | 3,215 |
| Below Average | 9,997 | 20.0% | 15,059 | 3,070 |
| High Usage | 5,002 | 10.0% | 18,219 | 3,705 |
| Low Usage | 5,001 | 10.0% | 13,074 | 2,671 |

### Top Performers by Segment
| Customer Segment | Customer Count | Percentage | Avg Usage (kWh) | Avg Revenue ($) |
|------------------|----------------|------------|-----------------|-----------------|
| Residential - Top 10% | 3,774 | 75.1% | 11,303 | 2,304 |
| Small Commercial - Top 10% | 1,191 | 23.7% | 30,555 | 6,208 |
| Large Commercial - Top 10% | 61 | 1.2% | 207,236 | 41,954 |

## Key Insights Generated

- **Growth Patterns:** All customer segments showing consistent 30-50% year-over-year growth with residential leading adoption
- **Seasonal Behavior:** Winter usage peaks 10-12% above summer baselines, reflecting heating vs cooling patterns typical of California climate
- **Program Effectiveness:** Clean energy programs showing measurable impact with 8-12% usage reductions in participating customers
- **Customer Segmentation:** Clear technology adoption patterns from "Full Tech Adopter" to "No Tech" enabling targeted program development
- **Geographic Variance:** City-level performance differences of 20-30% indicating market penetration opportunities

## Technologies Used

- **Google BigQuery** - Data warehouse and analytics platform
- **SQL** - Advanced analytics and data manipulation
- **Google Cloud Storage** - Data staging and export
- **Command Line Tools** - Automated query execution and results export

## Future Enhancements

- Real-time dashboard integration with BigQuery BI Engine
- Machine learning models for demand forecasting
- Automated alerting for usage anomalies

---

*This project demonstrates production-ready analytics for utility operations, showcasing both technical SQL proficiency and domain expertise in energy sector data analysis.*
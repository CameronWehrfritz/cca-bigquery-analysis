# CCA BigQuery Analytics

## About CCAs
Community Choice Aggregators (CCAs) are public agencies that provide electricity to customers within their jurisdictions, typically focusing on renewable energy and community benefits.

## Project Overview

This project demonstrates enterprise-level data analytics and automation for CCA utility operations using Google BigQuery and Google Cloud Platform. The analysis portfolio showcases advanced SQL techniques, modern analytics engineering practices, and production-ready cloud automation applied to synthetic energy usage data, providing actionable insights for utility operations, customer management, and regulatory compliance.

## Dataset Description

The synthetic dataset models a realistic CCA serving Peninsula Clean Energy's territory with:

- **39.59 million daily usage records** spanning January 2022 to August 2024
- **50,000 customers** across residential, small commercial, and large commercial segments
- **Customer demographics** including solar adoption, EV ownership, battery storage, and low-income qualifications
- **Program enrollments** tracking participation in clean energy incentive programs
- **Rate plans** reflecting actual CCA pricing structures (ECO, ECO100, ECOplus)

## Technical Architecture

**Platform:** Google BigQuery with modern ELT architecture and serverless automation

**Data Volume:** 39.59M records across 3 tables

**Storage Strategy:** Tables partitioned by date and clustered by customer demographics for optimized query performance

**Analytics Layer:** dbt-powered data marts for business intelligence

**Automation Layer:** Cloud Functions for automated monitoring and alerting

**ELT Pipeline with Layered Architecture:**
- **Raw Data Layer**: `cca_demo` dataset (synthetic customer and usage data)
- **Analytics Layer**: `cca_demo_dbt` dataset (dbt-generated data marts)
- **Automation Layer**: `cloud-functions` (serverless monitoring and alerting)

## Technologies Used

- **Data Warehouse**: Google BigQuery
- **Analytics Engineering**: dbt (data build tool)
- **Cloud Automation**: Google Cloud Functions
- **Data Modeling**: SQL with partitioning and clustering optimization
- **Cloud Platform**: Google Cloud Platform (GCP)
- **Version Control**: Git/GitHub

## Project Structure

```
├── cca_analytics_dbt/                  # dbt project
│   ├── models/
│   │   └── city_usage_summary.sql      # Data mart: usage by city & customer type
│   └── dbt_project.yml                 # dbt configuration
├── queries/
│   ├── 01_monthly_trends_analysis.sql
│   ├── 02_seasonal_analysis.sql
│   ├── 03_customer_ranking_analysis.sql
│   ├── 04_advanced_multi_table_analysis.sql
│   ├── 05_monthly_trends_alert.sql     # Automated alerting query
│   ├── summaries/
│   │   └── 03_customer_ranking_summary.sql
│   └── archive/
│       ├── 00_data_exploration.sql
│       └── 01_basic_exploration.sql
├── cloud-functions/
│   └── usage-alerts/                   # Serverless alerting system
│       ├── main.py                     # Cloud Function implementation
│       └── requirements.txt            # Python dependencies
├── data/
│   └── synthetic_data_creation.sql
└── README.md
```

## Analytics Portfolio

### 1. Monthly Trends Analysis
- **Purpose:** Year-over-year and month-over-month growth analysis
- **Techniques:** LAG window functions, CTEs, seasonal pattern detection
- **Business Value:** Capacity planning, regulatory reporting, growth forecasting
- **Key Finding:** Consistent 30-50% YoY growth across all customer segments

### 2. Seasonal Pattern Analysis
- **Purpose:** Advanced time-series analysis with volatility smoothing
- **Techniques:** Moving averages, conditional aggregations, variance calculations
- **Business Value:** Demand response planning, weather correlation, anomaly detection
- **Key Finding:** Clear seasonal patterns with winter usage peaks and summer efficiency valleys

### 3. Customer Ranking Analysis
- **Purpose:** Multi-dimensional customer segmentation and risk assessment
- **Techniques:** Multiple window function types, percentile analysis, dynamic thresholds
- **Business Value:** Account management, retention programs, targeted marketing
- **Key Finding:** Automated identification of high-value customers and churn risks

### 4. Advanced Multi-Table Analysis
- **Purpose:** Comprehensive customer lifecycle and program effectiveness measurement
- **Techniques:** Complex JOINs, before/after analysis, city benchmarking, technology profiling
- **Business Value:** Program ROI measurement, geographic performance analysis, opportunity identification
- **Key Finding:** Quantified program impacts showing 8-12% usage reductions post-enrollment

### 5. Automated Usage Trends Alerting
- **Purpose:** Production-ready monitoring system for anomalous growth patterns
- **Techniques:** Cloud Functions, serverless automation, threshold-based alerting
- **Business Value:** Proactive capacity planning, automated anomaly detection, operational efficiency
- **Implementation:** Serverless function that monitors YoY growth rates and triggers alerts for values >60% (high growth) or <10% (low growth)

## Cloud Automation

### Serverless Alerting System
Built with Google Cloud Functions to provide automated monitoring of usage trends:

**Features:**
- Automated detection of unusual year-over-year growth patterns
- Threshold-based alerting (>60% high growth, <10% low growth warnings)
- JSON API responses with structured alert data
- Production-ready error handling and logging
- Integration with BigQuery for real-time data analysis

**Architecture:**
- **Trigger:** HTTP endpoint (ready for Cloud Scheduler integration)
- **Compute:** Python 3.11 Cloud Function with 512MB memory
- **Data Source:** BigQuery with optimized query performance
- **Output:** Structured JSON with alert details and business context

**Alert Detection Logic:**
```python
# Example alert response for anomalous growth
{
  "status": "success",
  "message": "Sent 1 alerts",
  "alerts": [{
    "customer_type": "Small Commercial",
    "yoy_change_pct": 9.55,
    "alert_status": "LOW_GROWTH_ALERT",
    "alert_severity": "WARNING",
    "alert_message": "Small Commercial segment: 9.6% YoY growth (33800918 kWh total)"
  }]
}
```

## Modern Analytics Engineering with dbt

### Data Engineering Features
- **Partitioned Tables**: `daily_usage_facts` partitioned by `usage_date` and clustered by `customer_type` and `city` for optimal query performance
- **Synthetic Data Generation**: Realistic customer energy usage patterns with demographic and behavioral attributes
- **Performance Optimization**: Strategic use of partitioning and clustering for large-scale data analysis

### dbt Implementation
- **Modern ELT Architecture**: Raw data transformation using dbt for scalable analytics
- **Data Marts**: Business-focused aggregations for improved query performance
- **Automated Dependencies**: dbt manages transformation dependencies and materialization

### dbt Models

#### `city_usage_summary`
Data mart aggregating customer usage patterns by geography and customer type:
- Customer counts by city and segment
- Average daily usage (kWh) with explicit unit labeling
- Annual cost summaries
- Rate analysis across regions

#### `executive_summary`  
High-level business metrics view built from city usage data:
- Total cities and customer segments served
- Overall customer count and average usage
- Peak and lowest usage segments identified
- Demonstrates dbt dependency management with `{{ ref() }}`

| Metric | Value |
|--------|-------|
| Total Cities | 8 |
| Customer Segments | 3 |
| Total Customers | 50,000 |
| Overall Average Usage | 318 kWh |
| Peak Segment Usage | 840 kWh (Large Commercial) |
| Lowest Segment Usage | 31 kWh (Residential) |

**Usage patterns by customer segment:**
- **Large Commercial**: ~840 kWh daily average usage
- **Small Commercial**: ~124 kWh daily average usage  
- **Residential**: ~31 kWh daily average usage
- **Rate Consistency**: ~$0.20/kWh across most customer segments

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

**Cloud Architecture:**
- Serverless automation with Cloud Functions
- IAM-secured BigQuery integration
- Production-ready error handling and monitoring
- Scalable event-driven architecture

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
- Automated anomaly detection and alerting

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

## Setup Instructions

### Prerequisites
- Google Cloud Platform account with BigQuery access
- BigQuery command-line tool (`bq`) installed
- Google Cloud SDK (`gcloud`) for Cloud Functions deployment
- **Dataset Generation:** Run `data/synthetic_data_creation.sql` to create the CCA synthetic dataset (39.59M records across 3 tables)

### dbt Setup
1. **Virtual Environment**: Create and activate Python virtual environment
2. **Install dbt**: `pip install dbt-bigquery`
3. **Authentication**: Configure service account with BigQuery permissions
4. **Initialize**: Run `dbt init` and configure profiles
5. **Model Execution**: Run `dbt run` to create analytics layer

### Cloud Functions Deployment
1. **Navigate to function directory**: `cd cloud-functions/usage-alerts`
2. **Deploy function**: 
   ```bash
   gcloud functions deploy usage-trends-alert \
     --runtime python311 \
     --trigger-http \
     --allow-unauthenticated \
     --entry-point usage_trends_alert \
     --memory 512MB \
     --timeout 300s
   ```
3. **Test function**: Visit the deployed function URL to trigger alert detection

### Running Traditional SQL Queries
```bash
# Navigate to project directory
cd cca-bigquery-analysis

# Run individual queries
bq query --use_legacy_sql=false --max_rows=1000 < queries/03_customer_ranking_analysis.sql

# Test alerting query
bq query --use_legacy_sql=false --max_rows=10 < queries/05_monthly_trends_alert.sql

# Export results to CSV  
bq query --use_legacy_sql=false --format=csv --max_rows=5000 < queries/03_customer_ranking_analysis.sql > customer_rankings.csv

# Save results for summary analysis
bq query --use_legacy_sql=false --destination_table=cca_demo.customer_rankings_results < queries/03_customer_ranking_analysis.sql
bq query --use_legacy_sql=false < queries/summaries/03_customer_ranking_summary.sql
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

## Key Insights Generated

- **Growth Patterns:** All customer segments showing consistent 30-50% year-over-year growth with residential leading adoption
- **Seasonal Behavior:** Winter usage peaks 10-12% above summer baselines, reflecting heating vs cooling patterns typical of California climate
- **Program Effectiveness:** Clean energy programs showing measurable impact with 8-12% usage reductions in participating customers
- **Customer Segmentation:** Clear technology adoption patterns from "Full Tech Adopter" to "No Tech" enabling targeted program development
- **Geographic Variance:** City-level performance differences of 20-30% indicating market penetration opportunities
- **Operational Monitoring:** Automated detection of growth anomalies enables proactive capacity planning and operational response

## Future Enhancements

- Cloud Scheduler integration for automated monthly alert execution
- Email and Slack notification integration for alert distribution
- Additional dbt models for customer risk segmentation
- Real-time dashboard integration with BigQuery BI Engine
- Machine learning models for demand forecasting

---

*This project demonstrates production-ready analytics and automation for utility operations, showcasing SQL proficiency, modern analytics engineering practices, and cloud automation capabilities with domain expertise in energy sector data analysis.*
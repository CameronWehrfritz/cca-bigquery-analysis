# CCA BigQuery Analytics

## About CCAs
Community Choice Aggregators (CCAs) are public agencies that provide electricity to customers within their jurisdictions, typically focusing on renewable energy and community benefits.

## Project Overview

This project demonstrates enterprise-level data analytics and automation for CCA utility operations using Google BigQuery and Google Cloud Platform. The analysis portfolio showcases advanced SQL techniques, modern analytics engineering practices, and production-ready cloud automation applied to synthetic energy usage data, providing actionable insights for utility operations, customer management, and regulatory compliance.

## Dataset Description

The synthetic dataset models a realistic CCA serving Peninsula Clean Energy's territory with:

- **39.6 million daily usage records** spanning January 2022 to August 2024
- **50,000 customers** across residential, small commercial, and large commercial segments
- **Customer demographics** including solar adoption, EV ownership, battery storage, and low-income qualifications
- **Program enrollments** tracking participation in clean energy incentive programs
- **Rate plans** reflecting actual CCA pricing structures (ECO, ECO100, ECOplus)
- **Proper data modeling** with documented primary and foreign key relationships for data integrity

## Technical Architecture

**Platform:** Google BigQuery with modern ELT architecture and serverless automation

**Data Volume:** 39.6M records across 4 tables with proper relational structure

**Storage Strategy:** Tables partitioned by date and clustered by customer demographics for optimized query performance

**Data Modeling:** Documented primary and foreign keys with referential integrity design

**Analytics Layer:** dbt-powered data marts for business intelligence

**Automation Layer:** Cloud Functions and Cloud Scheduler for automated monitoring and alerting

**ELT Pipeline with Layered Architecture:**
- **Raw Data Layer**: `cca_demo` dataset (synthetic customer and usage data)
- **Analytics Layer**: `cca_demo_dbt` dataset (dbt-generated data marts)
- **Automation Layer**: `cloud-functions` (serverless monitoring and alerting)

## Technologies Used

- **Data Warehouse**: Google BigQuery
- **Analytics Engineering**: dbt (data build tool)
- **Cloud Automation**: Google Cloud Functions, Cloud Scheduler
- **Data Modeling**: SQL with partitioning, clustering, and primary/foreign key documentation
- **Cloud Platform**: Google Cloud Platform (GCP)
- **Version Control**: Git/GitHub

## Project Structure

```
├── cca_analytics_dbt/                  # dbt project
│   ├── models/
│   │   ├── customer_rankings.sql          # Customer analytics and segmentation
│   │   ├── city_usage_summary.sql         # Geographic usage aggregations
│   │   ├── executive_summary.sql          # Executive dashboard metrics
│   │   └── schema.yml                     # Source definitions and tests
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
├── data_setup/                         # Modular database setup scripts
│   ├── 01_create_customers.sql         # Customer dimension with primary key
│   ├── 02_create_usage_table_structure.sql  # Usage facts table structure
│   ├── 03_populate_usage_facts.sql     # Usage data population
│   ├── 04_create_programs.sql          # Program enrollments
│   └── 05_create_summary_stats.sql     # Data summary and quality metrics
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
- **Techniques:** Cloud Functions, Cloud Scheduler, serverless automation, threshold-based alerting
- **Business Value:** Proactive capacity planning, automated anomaly detection, operational efficiency
- **Implementation:** Scheduled serverless function that monitors YoY growth rates and triggers alerts for values >60% (high growth) or <10% (low growth)

## Cloud Automation

### Serverless Alerting System
Built with Google Cloud Functions and Cloud Scheduler to provide fully automated monitoring of usage trends:

**Features:**
- Automated detection of unusual year-over-year growth patterns
- Threshold-based alerting (>60% high growth, <10% low growth warnings)
- JSON API responses with structured alert data
- Production-ready error handling and logging
- Integration with BigQuery for real-time data analysis
- Scheduled monthly execution with Cloud Scheduler

**Architecture:**
- **Trigger:** Cloud Scheduler (monthly on 1st at 9 AM PST)
- **Compute:** Python 3.11 Cloud Function with 512MB memory
- **Data Source:** BigQuery with optimized query performance
- **Output:** Structured JSON with alert details and business context
- **Automation:** Event-driven architecture requiring zero manual intervention

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
- **Hybrid Schema Design**: Constellation schema with multiple fact tables (`daily_usage_facts`, `program_enrollments`) and central customer dimension (`customers`)
- **Strategic Denormalization**: Key customer attributes (type, city, rate_plan) denormalized in fact table for query performance while maintaining referential integrity
- **Partitioned Tables**: `daily_usage_facts` partitioned by `usage_date` and clustered by `customer_type` and `city` for optimal query performance
- **Documented Relationships**: Primary and foreign key constraints documented for data integrity and query optimization
- **Performance Optimization**: Balance between normalization (data integrity) and denormalization (query speed) for large-scale analytics

### dbt Implementation
- **Modern ELT Architecture**: Raw data transformation using dbt for scalable analytics
- **Three-Tier Pipeline**: Customer analytics foundation (customer_rankings), geographic aggregations (city_usage_summary), and executive metrics (executive_summary) with automated dependency management
- **Automated Dependencies**: dbt manages transformation dependencies and materialization

### dbt Models

#### `customer_rankings`
Foundational analytical model providing comprehensive customer segmentation and risk assessment:
- Multi-dimensional customer rankings (overall, by segment, by city)
- Usage patterns including seasonal and weekend/weekday analysis
- Financial metrics with effective rate calculations
- Risk indicators for churn prediction and account management
- Customer value segments (High Usage, Above Average, etc.)
- Technology adoption profiling (solar, EV, battery storage combinations)
- Percentile analysis for benchmarking within customer segments
- Year-to-date usage volatility and behavioral pattern detection

This model materializes as a table (50k rows) serving as the analytical foundation for downstream aggregations and business intelligence applications.

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

| Metric | Value |
|--------|-------|
| Total Cities | 8 |
| Customer Segments | 3 |
| Total Customers | 50,000 |
| Overall Average Usage | 318 kWh |
| Peak Segment Usage | 840 kWh (Large Commercial) |
| Lowest Segment Usage | 31 kWh (Residential) |

**Usage patterns by customer segment:**
- Large Commercial: ~840 kWh daily average usage
- Small Commercial: ~124 kWh daily average usage  
- Residential: ~31 kWh daily average usage
- Rate Consistency: ~$0.20/kWh across most customer segments

**Pipeline Architecture:** Models demonstrate proper dbt dependency management with `{{ ref() }}` functions, ensuring automatic execution ordering and data lineage tracking.

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

**Data Modeling:**
- Documented primary and foreign key relationships
- Referential integrity design principles
- Composite keys for fact table uniqueness
- Proper normalization and denormalization strategies

**Cloud Architecture:**
- Serverless automation with Cloud Functions and Cloud Scheduler
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

### Database Setup
The database creation uses a modular approach with 5 focused scripts:

1. **Create customer dimension table:**
   ```bash
   bq query --use_legacy_sql=false < data_setup/01_create_customers.sql
   ```

2. **Create usage facts table structure:**
   ```bash
   bq query --use_legacy_sql=false < data_setup/02_create_usage_table_structure.sql
   ```

3. **Populate usage facts (39.6M records):**
   ```bash
   bq query --use_legacy_sql=false < data_setup/03_populate_usage_facts.sql
   ```

4. **Create program enrollments:**
   ```bash
   bq query --use_legacy_sql=false < data_setup/04_create_programs.sql
   ```

5. **Generate summary statistics:**
   ```bash
   bq query --use_legacy_sql=false < data_setup/05_create_summary_stats.sql
   ```

**Note:** Scripts must be run in order due to foreign key dependencies.

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

### Cloud Scheduler Setup
1. **Create scheduled job**:
   ```bash
   gcloud scheduler jobs create http monthly-usage-alert \
     --schedule="0 9 1 * *" \
     --uri=YOUR_CLOUD_FUNCTION_URL \
     --http-method=GET \
     --time-zone="America/Los_Angeles" \
     --description="Monthly CCA usage trends alert" \
     --location=us-central1
   ```
2. **Test scheduled execution**: `gcloud scheduler jobs run monthly-usage-alert --location=us-central1`

### Running Traditional SQL Queries
```bash
# Navigate to project directory
cd cca-bigquery-analysis

# Run individual queries
bq query --use_legacy_sql=false --max_rows=1000 < queries/01_monthly_trends_analysis.sql

# Export results to CSV  
bq query --use_legacy_sql=false --format=csv --max_rows=5000 < queries/03_customer_ranking_analysis.sql > customer_rankings.csv

# Test alerting query
bq query --use_legacy_sql=false --max_rows=10 < queries/05_monthly_trends_alert.sql
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

- Email and Slack notification integration for alert distribution  
- Additional dbt models for customer risk segmentation
- Real-time dashboard integration with BigQuery BI Engine
- Machine learning models for demand forecasting

---

*This project demonstrates production-ready analytics and automation for utility operations, showcasing SQL proficiency, modern analytics engineering practices, and cloud automation capabilities with domain expertise in energy sector data analysis.*
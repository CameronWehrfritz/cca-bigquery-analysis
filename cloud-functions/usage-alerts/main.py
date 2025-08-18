import functions_framework
from google.cloud import bigquery
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def usage_trends_alert(request):
    """
    Cloud Function to check usage trends and send email alerts
    Triggered by Cloud Scheduler monthly
    """
    try:
        logger.info("Starting usage trends alert check")
        
        # Initialize BigQuery client
        client = bigquery.Client()
        
        # The alert query (paste the SQL from above here)
        alert_query = """
        WITH monthly_data AS (
          SELECT
            DATE_TRUNC(usage_date, MONTH) as usage_month,
            customer_type,
            ROUND(SUM(kwh_used), 2) as monthly_kwh,
            COUNT(DISTINCT customer_id) as customers_active,
            ROUND(SUM(kwh_used) / COUNT(DISTINCT customer_id), 2) as avg_usage_per_customer
          FROM `cca-bigquery-analytics.cca_demo.daily_usage_facts`
          WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH)
          GROUP BY DATE_TRUNC(usage_date, MONTH), customer_type
        ),
        trend_analysis AS (
          SELECT
            usage_month,
            customer_type,
            monthly_kwh as current_month_kwh,
            customers_active,
            avg_usage_per_customer,
            LAG(monthly_kwh, 12) OVER (
              PARTITION BY customer_type
              ORDER BY usage_month
            ) as previous_year_month_kwh,
            ROUND(
              (monthly_kwh - LAG(monthly_kwh, 12) OVER (
                PARTITION BY customer_type
                ORDER BY usage_month
              )) / NULLIF(LAG(monthly_kwh, 12) OVER (
                PARTITION BY customer_type
                ORDER BY usage_month
              ), 0) * 100, 2
            ) as yoy_change_pct
          FROM monthly_data
        ),
        alerts AS (
          SELECT 
            *,
            CASE 
              WHEN yoy_change_pct > 60 THEN 'HIGH_GROWTH_ALERT'
              WHEN yoy_change_pct < 10 THEN 'LOW_GROWTH_ALERT'
              WHEN yoy_change_pct IS NULL THEN 'INSUFFICIENT_DATA'
              ELSE 'NORMAL'
            END as alert_status,
            CASE 
              WHEN yoy_change_pct > 60 THEN 'CRITICAL'
              WHEN yoy_change_pct < 10 THEN 'WARNING'
              ELSE 'INFO'
            END as alert_severity
          FROM trend_analysis
          WHERE usage_month = '2024-08-01'
            AND yoy_change_pct IS NOT NULL
        )
        SELECT 
          usage_month,
          customer_type,
          current_month_kwh,
          customers_active,
          yoy_change_pct,
          alert_status,
          alert_severity,
          CONCAT(
            customer_type, ' segment: ', 
            CAST(ROUND(yoy_change_pct, 1) AS STRING), '% YoY growth (',
            CAST(ROUND(current_month_kwh, 0) AS STRING), ' kWh total)'
          ) as alert_message
        FROM alerts 
        WHERE alert_status IN ('HIGH_GROWTH_ALERT', 'LOW_GROWTH_ALERT')
        ORDER BY 
          CASE alert_severity 
            WHEN 'CRITICAL' THEN 1 
            WHEN 'WARNING' THEN 2 
            ELSE 3 
          END,
          customer_type
        """
        
        # Execute query
        logger.info("Executing BigQuery alert analysis")
        query_job = client.query(alert_query)
        results = query_job.to_dataframe()
        
        if results.empty:
            logger.info("No alerts triggered - all growth rates within normal range")
            return {
                'status': 'success',
                'message': 'No alerts triggered',
                'timestamp': datetime.now().isoformat()
            }
        
        # Send email alert
        logger.info(f"Found {len(results)} alerts to send")
        send_email_alert(results)
        
        return {
            'status': 'success',
            'message': f'Sent {len(results)} alerts',
            'alerts': results.to_dict('records'),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in usage trends alert: {str(e)}")
        return {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }, 500

def send_email_alert(alert_data):
    """
    Send email alert with usage trend warnings
    """
    # Email configuration from environment variables
    smtp_server = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    email_user = os.environ.get('EMAIL_USER')
    email_password = os.environ.get('EMAIL_PASSWORD')
    alert_recipients = os.environ.get('ALERT_RECIPIENTS', '').split(',')
    
    if not email_user or not email_password:
        logger.warning("Email credentials not configured - skipping email send")
        return
    
    # Create email content
    subject = f"🚨 CCA Usage Trends Alert - {len(alert_data)} segments need attention"
    
    # Build HTML email body
    body_html = f"""
    <html>
    <body>
        <h2>CCA Usage Trends Alert</h2>
        <p>The following customer segments have unusual year-over-year growth patterns:</p>
        
        <table border="1" style="border-collapse: collapse; margin: 20px 0;">
            <tr style="background-color: #f2f2f2;">
                <th>Customer Type</th>
                <th>YoY Growth</th>
                <th>Current Month kWh</th>
                <th>Active Customers</th>
                <th>Alert Level</th>
            </tr>
    """
    
    for _, row in alert_data.iterrows():
        severity_color = '#ff4444' if row['alert_severity'] == 'CRITICAL' else '#ffaa00'
        body_html += f"""
            <tr>
                <td>{row['customer_type']}</td>
                <td style="color: {severity_color}; font-weight: bold;">{row['yoy_change_pct']}%</td>
                <td>{row['current_month_kwh']:,.0f}</td>
                <td>{row['customers_active']:,}</td>
                <td style="color: {severity_color};">{row['alert_severity']}</td>
            </tr>
        """
    
    body_html += """
        </table>
        
        <h3>Recommended Actions:</h3>
        <ul>
            <li><strong>HIGH GROWTH (>60%):</strong> Review capacity planning and procurement contracts</li>
            <li><strong>LOW GROWTH (<10%):</strong> Investigate potential customer churn or market changes</li>
        </ul>
        
        <p><em>Generated automatically by CCA Analytics System</em></p>
    </body>
    </html>
    """
    
    # Send email
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email_user
    msg['To'] = ', '.join(alert_recipients)
    
    html_part = MIMEText(body_html, 'html')
    msg.attach(html_part)
    
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(email_user, email_password)
            server.send_message(msg)
        
        logger.info(f"Alert email sent successfully to {alert_recipients}")
        
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        raise
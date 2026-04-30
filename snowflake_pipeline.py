"""
snowflake_pipeline.py
─────────────────────
Connects to Snowflake, queries the Analytics layer,
exports results to CSV files for Power BI consumption.

Usage:
    1. Copy .env.example → .env and fill in your credentials
    2. pip install -r requirements.txt
    3. python snowflake_pipeline.py
"""

import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# ── Load credentials from .env ─────────────────────────────────────────────
load_dotenv()

SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),   # e.g. abc123.ap-southeast-1
    "warehouse": "COMPUTE_WH",
    "database":  "HEALTHCARE_DWH",
    "schema":    "ANALYTICS",
}

# ── Queries to export ──────────────────────────────────────────────────────
QUERIES = {
    "revenue_by_condition":  "SELECT * FROM vw_revenue_by_condition",
    "admission_analysis":    "SELECT * FROM vw_admission_analysis",
    "billing_anomalies":     "SELECT * FROM vw_billing_anomalies",
    "insurance_performance": "SELECT * FROM vw_insurance_performance",
    "monthly_trend":         "SELECT * FROM vw_monthly_trend",
    "doctor_performance":    "SELECT * FROM vw_doctor_performance",
}

OUTPUT_DIR = "exports"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_pipeline():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    print("Connected.\n")

    results = {}

    for name, query in QUERIES.items():
        print(f"Running: {name}...")
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        results[name] = df

        output_path = f"{OUTPUT_DIR}/{name}.csv"
        df.to_csv(output_path, index=False)
        print(f"  → {len(df)} rows exported to {output_path}")

    cursor.close()
    conn.close()
    print("\nAll exports complete. Ready for Power BI.")

    # ── Print key findings ─────────────────────────────────────────────────
    print("\n" + "="*55)
    print("KEY FINDINGS")
    print("="*55)

    rev = results["revenue_by_condition"]
    top = rev.iloc[0]
    print(f"Top Revenue Segment : {top['MEDICAL_CONDITION']} + {top['INSURANCE_PROVIDER']}")
    print(f"Total Revenue       : ${top['TOTAL_REVENUE']:,.2f}")

    adm = results["admission_analysis"]
    urgent = adm[adm["ADMISSION_TYPE"].str.upper() == "URGENT"]
    if not urgent.empty:
        print(f"Urgent Admissions   : {urgent['PCT_OF_TOTAL'].values[0]}% of total")

    anomalies = results["billing_anomalies"]
    print(f"Billing Anomalies   : {len(anomalies)} records with billing <= 0")
    if len(anomalies) > 0:
        print(f"Total Negative Amt  : ${anomalies['BILLING_AMOUNT'].sum():,.2f}")

    print("="*55)


if __name__ == "__main__":
    run_pipeline()

-- ============================================================
-- 03_analytics_views.sql
-- Gold Layer: Analytical Views for Power BI & reporting
-- ============================================================

USE DATABASE HEALTHCARE_DWH;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- ── VIEW 1: Revenue by Medical Condition & Insurance Provider ──────────────
CREATE OR REPLACE VIEW vw_revenue_by_condition AS
SELECT
    medical_condition,
    insurance_provider,
    COUNT(*)                                            AS total_patients,
    ROUND(SUM(billing_amount), 2)                       AS total_revenue,
    ROUND(AVG(billing_amount), 2)                       AS avg_billing,
    ROUND(MIN(billing_amount), 2)                       AS min_billing,
    ROUND(MAX(billing_amount), 2)                       AS max_billing,
    RANK() OVER (ORDER BY SUM(billing_amount) DESC)     AS revenue_rank
FROM HEALTHCARE_DWH.RAW.PATIENTS
WHERE billing_amount > 0
GROUP BY 1, 2
ORDER BY revenue_rank;

-- ── VIEW 2: Admission Type Analysis ───────────────────────────────────────
CREATE OR REPLACE VIEW vw_admission_analysis AS
SELECT
    admission_type,
    COUNT(*)                                                        AS total_admissions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)             AS pct_of_total,
    ROUND(AVG(length_of_stay), 1)                                   AS avg_stay_days,
    ROUND(SUM(billing_amount), 2)                                   AS total_revenue,
    ROUND(AVG(billing_amount), 2)                                   AS avg_revenue_per_admission
FROM HEALTHCARE_DWH.RAW.PATIENTS
GROUP BY 1
ORDER BY total_admissions DESC;

-- ── VIEW 3: Billing Anomaly Detection ─────────────────────────────────────
CREATE OR REPLACE VIEW vw_billing_anomalies AS
SELECT
    patient_id,
    name,
    medical_condition,
    insurance_provider,
    billing_amount,
    admission_type,
    date_of_admission
FROM HEALTHCARE_DWH.RAW.PATIENTS
WHERE billing_amount <= 0
ORDER BY billing_amount ASC;

-- ── VIEW 4: Insurance Provider Performance ────────────────────────────────
CREATE OR REPLACE VIEW vw_insurance_performance AS
SELECT
    insurance_provider,
    COUNT(*)                                        AS total_claims,
    ROUND(SUM(billing_amount), 2)                   AS total_payout,
    ROUND(AVG(billing_amount), 2)                   AS avg_claim_amount,
    ROUND(AVG(length_of_stay), 1)                   AS avg_stay_days,
    RANK() OVER (ORDER BY SUM(billing_amount) DESC) AS payout_rank
FROM HEALTHCARE_DWH.RAW.PATIENTS
WHERE billing_amount > 0
GROUP BY 1
ORDER BY payout_rank;

-- ── VIEW 5: Monthly Admission Trend ───────────────────────────────────────
CREATE OR REPLACE VIEW vw_monthly_trend AS
SELECT
    DATE_TRUNC('month', date_of_admission)          AS admission_month,
    admission_type,
    COUNT(*)                                        AS total_admissions,
    ROUND(SUM(billing_amount), 2)                   AS monthly_revenue,
    ROUND(AVG(billing_amount), 2)                   AS avg_billing
FROM HEALTHCARE_DWH.RAW.PATIENTS
WHERE billing_amount > 0
GROUP BY 1, 2
ORDER BY 1, 2;

-- ── VIEW 6: Top Doctors by Patient Volume ─────────────────────────────────
CREATE OR REPLACE VIEW vw_doctor_performance AS
SELECT
    doctor,
    COUNT(*)                                        AS total_patients,
    ROUND(SUM(billing_amount), 2)                   AS total_revenue,
    ROUND(AVG(billing_amount), 2)                   AS avg_billing,
    ROUND(AVG(length_of_stay), 1)                   AS avg_stay_days,
    RANK() OVER (ORDER BY COUNT(*) DESC)            AS patient_volume_rank
FROM HEALTHCARE_DWH.RAW.PATIENTS
GROUP BY 1
ORDER BY patient_volume_rank;

-- ── Verify all views ──────────────────────────────────────────────────────
SELECT * FROM vw_revenue_by_condition       LIMIT 5;
SELECT * FROM vw_admission_analysis         LIMIT 5;
SELECT * FROM vw_billing_anomalies          LIMIT 5;
SELECT * FROM vw_insurance_performance      LIMIT 5;
SELECT * FROM vw_monthly_trend              LIMIT 5;
SELECT * FROM vw_doctor_performance         LIMIT 5;

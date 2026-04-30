-- ============================================================
-- 01_setup.sql
-- Create Database, Schemas, and Warehouse
-- ============================================================

-- Create Database
CREATE DATABASE IF NOT EXISTS HEALTHCARE_DWH;

-- Create Schemas (Medallion Architecture)
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DWH.RAW;       -- Bronze: raw ingested data
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DWH.ANALYTICS; -- Gold:   clean analytical views

-- Create Virtual Warehouse (auto-suspends after 60s to save credits)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE  = 'X-SMALL'
  AUTO_SUSPEND    = 60
  AUTO_RESUME     = TRUE
  COMMENT         = 'Warehouse for Healthcare DWH project';

-- Set context
USE DATABASE HEALTHCARE_DWH;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

# POC Snowflake-Databricks Pipeline - Final Scripts

This folder contains the **final, proven scripts** that achieved perfect platform parity in our POC.

## 📋 Execution Order

Run these scripts in this exact order:

### 1. `01_staging_to_bronze_loader.sql`
**Purpose**: Load raw production files into structured bronze table
- **Input**: Raw files from @NCP stage (Sept 4-6, 2025)  
- **Output**: `poc.public.ncp_bronze` with 185 parsed columns
- **Result**: 36,349,536 records across 283 files loaded

### 2. `02_bronze_to_silver_sept5.sql` 
**Purpose**: Transform bronze data into clean silver table for Sept 5th
- **Input**: `poc.public.ncp_bronze` 
- **Output**: `poc.public.ncp_silver_sept5_test`
- **Result**: 12,686,818 processed records with Databricks parity

### 3. `03_cross_platform_validation.sql`
**Purpose**: 6-level progressive validation framework 
- **Input**: `poc.public.ncp_silver_sept5_test`
- **Output**: Comprehensive validation reports
- **Result**: Perfect statistical parity achieved

## 🎯 Proven Results

- ✅ **Perfect 1:1 platform parity** - All 6 validation levels passed
- ✅ **Statistical alignment** - Q1=$7.05, Median=$19.99, Q3=$46.00
- ✅ **Boolean normalization** - Exact yes/no → true/false handling
- ✅ **Data quality** - Zero data loss, complete deduplication
- ✅ **Production scale** - 36M+ records processed successfully

## 📊 Key Metrics

| Metric | Snowflake | Databricks | Status |
|--------|-----------|------------|--------|
| Total Records | 12,686,818 | 12,686,818 | ✅ Perfect Match |
| Q1 Amount | $7.05 | $7.05 | ✅ Perfect Match |
| Median Amount | $19.99 | $19.99 | ✅ Perfect Match |
| Q3 Amount | $46.00 | $46.00 | ✅ Perfect Match |
| Zero Count | 133,433 | 133,433 | ✅ Perfect Match |
| Std Deviation | 396.909836034 | 396.9098360339789 | ✅ Perfect Match |

## 🔧 Technical Notes

- **Date Range**: September 4-6, 2025 (production data)
- **Validation Date**: September 5, 2025 (complete 24-hour coverage)
- **Test Client Filtering**: 4 test clients excluded consistently
- **Deduplication**: ROW_NUMBER() by transaction_main_id + transaction_date
- **Boolean Logic**: Comprehensive yes/no/true/false normalization

## 📁 Archive Information

These scripts represent the **final working state** of the POC after achieving perfect platform parity.
Original development scripts can be found in the parent directory but should not be used for production.

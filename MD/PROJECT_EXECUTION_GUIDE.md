# POC SNOWFLAKE VS DATABRICKS - ORGANIZED PROJECT STRUCTURE

## 🚀 QUICK START - EXECUTION ORDER

### STEP 1: Create Bronze Table from Staging
```bash
# Run this first - creates bronze table with 185-column parsing from existing staging data
snowsql -c poc_connection -f "1_DATA_LOADING/create_bronze_from_staging.sql"
```

### STEP 2: Run ETL Processing  
```bash
# Run this second - processes bronze data to silver with all business logic
snowsql -c poc_connection -f "2_ETL_SCRIPTS/optimized_etl_FINAL.sql"
```

### STEP 3: Compare Results
- Check silver table results vs Databricks sample in `5_REFERENCE_DATA/`
- Measure performance vs Databricks timings

---

## 📁 PROJECT STRUCTURE

### 1_DATA_LOADING/ 
**Purpose**: Scripts for loading raw data from staging to bronze
- `create_bronze_from_staging.sql` ✅ **MAIN SCRIPT** - Use this one! (Creates bronze from existing staging)
- `copy_to_bronze_FINAL.sql` - Full staging + bronze creation (only if starting fresh)
- `optional_load_more_staging_data.sql` - Load additional data files to staging
- `initial_staging_setup.sql` - First-time staging setup
- `snowflake/refactored_scripts/create_proper_bronze.sql` - Sample data creation (for testing)

### 2_ETL_SCRIPTS/
**Purpose**: Main ETL processing scripts  
- `optimized_etl_FINAL.sql` ✅ **MAIN ETL** - Complete Databricks parity
- `snowflake/refactored_scripts/` - Alternative implementations (archived)

### 3_TESTING_DEBUG/
**Purpose**: Testing, debugging, and validation scripts
- `simple_etl_test.sql` - Simple test with 3 rows
- `debug_*.sql` - Various debugging scripts
- `count_databricks_columns.sql` - Column validation

### 4_DOCUMENTATION/
**Purpose**: Project documentation and status
- `POC_PROJECT_STATUS.md` - Current project status
- `ETL_Process_Visualization.html` - Visual process flow
- `POC_CLAUDE_INSTRUCTIONS.md` - Instructions
- `README.md` - Project overview

### 5_REFERENCE_DATA/
**Purpose**: Reference data and comparison baselines
- `databricks_silver_data_sample` - Exact Databricks output structure
- `databricks/` - Databricks reference files

---

## ⚠️ DEPRECATED/REDUNDANT FILES (To be cleaned up)

### Root Directory Cleanup Needed:
- `load_production_bronze.sql` ❌ DELETE (redundant)
- `complete_snowflake_etl.sql` ❌ DELETE (outdated) 
- `clean_exact_match.sql` ❌ DELETE (testing only)
- `exact_databricks_equivalent.sql` ❌ DELETE (outdated)
- `debug_simple.sql` ❌ MOVE to 3_TESTING_DEBUG/
- `debug_etl_filters.sql` ❌ MOVE to 3_TESTING_DEBUG/

---

## 🎯 CURRENT STATUS

✅ **COMPLETED**:
- Bronze table structure (185 columns)
- Complete ETL with Databricks business logic parity
- Sample data testing (3 rows working perfectly)
- Production data loaded in staging

🔄 **CURRENT STEP**: 
Load production data to bronze with proper 185-column parsing

🎯 **NEXT**: 
Run ETL on production data and measure performance vs Databricks

---

## 🔧 MAIN FILES TO USE

1. **`1_DATA_LOADING/copy_to_bronze_FINAL.sql`** - Load production data
2. **`2_ETL_SCRIPTS/optimized_etl_FINAL.sql`** - Process to silver  
3. **`5_REFERENCE_DATA/databricks_silver_data_sample`** - Compare results

Everything else is either deprecated, testing, or reference material.

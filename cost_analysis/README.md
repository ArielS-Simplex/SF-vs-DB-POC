# Cost Analysis Framework

This folder contains professional cost measurement tools for the Snowflake vs Databricks ETL POC.

## Files:

### 1. `simple_etl_cost_tracker.sql`
- **Purpose**: Easy before/after cost tracking
- **Usage**: Run Step 1 before ETL, then Step 2 after ETL
- **Best for**: Quick cost estimates

### 2. `cost_measurement_professional.sql` 
- **Purpose**: Comprehensive enterprise-grade cost analysis
- **Usage**: Run after completing full ETL pipeline
- **Measures**: Query history, warehouse usage, storage, data transfer
- **Best for**: Detailed cost breakdown by pipeline stage

### 3. `cost_comparison_framework.sql`
- **Purpose**: Snowflake vs Databricks comparison template
- **Usage**: Run after both platforms complete ETL
- **Provides**: Cost per record, efficiency metrics, comparative analysis
- **Best for**: POC decision-making

## ETL Pipeline for Cost Measurement:

1. **S3 → Staging**: `final/00_staging_data_loader.sql`
2. **Staging → Bronze**: `final/01_staging_to_bronze_loader.sql` 
3. **Bronze → Silver**: `snowflake/refactored_scripts/enhanced_working_etl.sql`

## Usage Workflow:

1. Run `simple_etl_cost_tracker.sql` (Step 1 - Baseline)
2. Execute complete ETL pipeline (3 stages above)
3. Run `cost_measurement_professional.sql` (Full analysis)
4. Run `cost_comparison_framework.sql` (Compare platforms)
5. Document results for POC decision

## Key Metrics to Track:

- Total ETL cost (USD)
- Cost per million records
- Execution time (minutes)
- Compute vs storage vs transfer costs
- Platform efficiency comparison
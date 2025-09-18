# Snowflake ETL Cost Calculator

An interactive web-based cost calculator for analyzing Snowflake ETL operations, built as part of the Databricks to Snowflake migration POC.

## 🚀 Live Demo

Visit the live calculator: [GitHub Pages URL]

## 📊 Features

- **Interactive Cost Calculator**: Calculate costs based on records, warehouse size, and Snowflake edition
- **Real-time Visualizations**: Dynamic charts showing cost breakdowns
- **Pipeline Stage Analysis**: Detailed breakdown of ETL pipeline costs
- **Responsive Design**: Works on desktop, tablet, and mobile devices
- **POC Results Integration**: Based on actual ETL performance data

## 💡 Key Metrics from POC

- **Records Processed**: 12,686,818
- **Total Execution Time**: 3.24 minutes
- **Credits Used**: 0.0017
- **Efficiency**: 0.000138 credits per million records

## 🛠️ Technologies Used

- **Frontend**: HTML5, CSS3, JavaScript (ES6+)
- **Charts**: Chart.js
- **Styling**: CSS Grid, Flexbox, CSS Animations
- **Hosting**: GitHub Pages

## 📈 Cost Analysis Features

### Calculator Inputs
- Number of records to process
- Warehouse size (X-Small to X-Large)
- Snowflake edition (Standard, Enterprise, Business Critical)
- ETL frequency (Daily, Weekly, Monthly)

### Outputs
- Cost per ETL run
- Monthly cost estimates
- Annual cost projections
- Credits per million records
- Efficiency metrics

### Pipeline Breakdown
- S3 to Staging costs
- Staging to Bronze costs
- Other operations costs

## 🚀 Setup for GitHub Pages

1. **Enable GitHub Pages**:
   - Go to repository Settings
   - Scroll to "Pages" section
   - Set source to "Deploy from a branch"
   - Select "main" branch and "/docs" folder
   - Save settings

2. **Update Configuration**:
   - Edit `docs/_config.yml`
   - Update `url` and `baseurl` with your GitHub Pages URL
   - Update `github_username` and `repository` name

3. **Access Your Site**:
   - URL format: `https://[username].github.io/[repository-name]/`
   - Example: `https://johndoe.github.io/POC_Snowflake_Databricks/`

## 📁 File Structure

```
docs/
├── index.html          # Main HTML page
├── styles.css          # CSS styling and animations
├── script.js           # JavaScript functionality
├── _config.yml         # Jekyll configuration
└── README.md           # This file
```

## 🔧 Customization

### Update Cost Data
Edit the `BASE_METRICS` object in `script.js`:
```javascript
const BASE_METRICS = {
    records: 12686818,
    credits: 0.0017,
    executionMinutes: 3.24,
    queries: 28
};
```

### Modify Credit Rates
Update the `CREDIT_RATES` object for different pricing:
```javascript
const CREDIT_RATES = {
    'standard': 2.00,
    'enterprise': 3.00,
    'business-critical': 4.00
};
```

### Add New Visualizations
Use Chart.js to add additional charts:
```javascript
new Chart(ctx, {
    type: 'bar', // or 'line', 'pie', etc.
    data: { /* your data */ },
    options: { /* chart options */ }
});
```

## 📊 Data Sources

The calculator is based on real POC data from:
- `cost_analysis/warehouse_compute_costs.sql`
- `cost_analysis/simple_etl_cost_tracker.sql`
- Actual Snowflake query history and metering data

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the calculator locally
5. Submit a pull request

## 📝 License

This project is part of an internal POC for Databricks to Snowflake migration analysis.

## 🔗 Related Documentation

- [Snowflake Pricing Guide](https://www.snowflake.com/pricing/)
- [ETL Performance Analysis](../DATABRICKS_SNOWFLAKE_ETL_DIFFERENCES.md)
- [POC Documentation](../CLAUDE.md)
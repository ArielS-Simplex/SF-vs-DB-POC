// Snowflake ETL Cost Calculator JavaScript

// Base cost rates and warehouse multipliers
const CREDIT_RATES = {
    'standard': 2.00,
    'enterprise': 3.00,
    'business-critical': 4.00
};

const WAREHOUSE_MULTIPLIERS = {
    'x-small': 1,
    'small': 2,
    'medium': 4,
    'large': 8,
    'x-large': 16
};

// Base metrics from POC results
const BASE_METRICS = {
    records: 12686818,
    credits: 0.0017,
    executionMinutes: 3.24,
    queries: 28
};

// Chart instance
let costChart = null;

function calculateCosts() {
    const records = parseInt(document.getElementById('records').value);
    const warehouseSize = document.getElementById('warehouse-size').value;
    const edition = document.getElementById('edition').value;
    const frequency = document.getElementById('frequency').value;
    
    // Calculate scaling factors
    const recordsRatio = records / BASE_METRICS.records;
    const warehouseMultiplier = WAREHOUSE_MULTIPLIERS[warehouseSize];
    const creditRate = CREDIT_RATES[edition];
    
    // Calculate scaled metrics
    const scaledCredits = BASE_METRICS.credits * recordsRatio * warehouseMultiplier;
    const scaledExecutionTime = BASE_METRICS.executionMinutes * recordsRatio / warehouseMultiplier;
    const costPerRun = scaledCredits * creditRate;
    
    // Calculate frequency costs
    const frequencyMultiplier = getFrequencyMultiplier(frequency);
    const monthlyCost = costPerRun * frequencyMultiplier;
    const annualCost = monthlyCost * 12;
    
    // Display results
    displayResults({
        records,
        credits: scaledCredits,
        executionTime: scaledExecutionTime,
        costPerRun,
        monthlyCost,
        annualCost,
        warehouseSize,
        edition,
        frequency
    });
    
    // Update chart
    updateCostChart({
        costPerRun,
        monthlyCost,
        annualCost
    });
}

function getFrequencyMultiplier(frequency) {
    switch(frequency) {
        case 'daily': return 30;
        case 'weekly': return 4;
        case 'monthly': return 1;
        default: return 30;
    }
}

function displayResults(results) {
    const resultsDiv = document.getElementById('results');
    
    resultsDiv.innerHTML = `
        <h3>💰 Cost Estimate Results</h3>
        <div class="results-grid">
            <div class="result-item">
                <label>Records to Process:</label>
                <span>${results.records.toLocaleString()}</span>
            </div>
            <div class="result-item">
                <label>Estimated Credits:</label>
                <span>${results.credits.toFixed(6)}</span>
            </div>
            <div class="result-item">
                <label>Execution Time:</label>
                <span>${results.executionTime.toFixed(2)} minutes</span>
            </div>
            <div class="result-item highlight">
                <label>Cost Per Run:</label>
                <span>$${results.costPerRun.toFixed(4)}</span>
            </div>
            <div class="result-item highlight">
                <label>Monthly Cost (${results.frequency}):</label>
                <span>$${results.monthlyCost.toFixed(2)}</span>
            </div>
            <div class="result-item highlight">
                <label>Annual Cost:</label>
                <span>$${results.annualCost.toFixed(2)}</span>
            </div>
        </div>
        
        <div class="efficiency-metrics">
            <h4>📊 Efficiency Metrics</h4>
            <p>Credits per Million Records: <strong>${(results.credits / (results.records / 1000000)).toFixed(6)}</strong></p>
            <p>Cost per Million Records: <strong>$${(results.costPerRun / (results.records / 1000000)).toFixed(4)}</strong></p>
            <p>Warehouse: ${results.warehouseSize.toUpperCase()} | Edition: ${results.edition.charAt(0).toUpperCase() + results.edition.slice(1)}</p>
        </div>
    `;
}

function updateCostChart(costs) {
    const ctx = document.getElementById('costChart').getContext('2d');
    
    if (costChart) {
        costChart.destroy();
    }
    
    costChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Per Run', 'Monthly', 'Annual'],
            datasets: [{
                data: [costs.costPerRun, costs.monthlyCost, costs.annualCost],
                backgroundColor: [
                    '#3498db',
                    '#e74c3c',
                    '#2ecc71'
                ],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 20,
                        font: {
                            size: 14
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': $' + context.parsed.toFixed(4);
                        }
                    }
                }
            }
        }
    });
}

// Initialize with default calculation
document.addEventListener('DOMContentLoaded', function() {
    calculateCosts();
    
    // Add event listeners for real-time updates
    document.getElementById('records').addEventListener('input', calculateCosts);
    document.getElementById('warehouse-size').addEventListener('change', calculateCosts);
    document.getElementById('edition').addEventListener('change', calculateCosts);
    document.getElementById('frequency').addEventListener('change', calculateCosts);
});

// Stage breakdown chart
function initializeStageChart() {
    const ctx = document.getElementById('stageChart');
    if (!ctx) return;
    
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['S3 to Staging', 'Staging to Bronze', 'Other Operations'],
            datasets: [{
                label: 'Execution Time (minutes)',
                data: [1.23, 1.94, 0.06],
                backgroundColor: '#3498db',
                borderColor: '#2980b9',
                borderWidth: 1
            }, {
                label: 'Credits Used',
                data: [0.001, 0.0002, 0.0005],
                backgroundColor: '#e74c3c',
                borderColor: '#c0392b',
                borderWidth: 1,
                yAxisID: 'y1'
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    grid: {
                        drawOnChartArea: false,
                    },
                }
            }
        }
    });
}

// Comparison utilities
function showComparison() {
    // Add interactive comparison features
    const comparisonData = {
        snowflake: {
            creditsPerMillion: 0.000138,
            predictability: 'High',
            scaling: 'Linear'
        },
        databricks: {
            creditsPerMillion: 'Variable',
            predictability: 'Medium',
            scaling: 'Cluster-based'
        }
    };
    
    console.log('Comparison data loaded:', comparisonData);
}
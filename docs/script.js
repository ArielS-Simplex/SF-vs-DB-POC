// Compact Calculator JavaScript

// Cost rates and warehouse multipliers
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

// Base metrics from ACTUAL WAREHOUSE METERING (7 ETL runs - bulletproof for managers)
const BASE_METRICS = {
    records: 12.686818, // millions
    credits: 0.100672, // ACTUAL compute + cloud services credits
    executionMinutes: 5.95,
    queries: 26,
    totalCostUSD: 0.3020 // REAL cost from Snowflake billing
};

function calculateCosts() {
    const recordsMillions = parseFloat(document.getElementById('records').value);
    const warehouseSize = document.getElementById('warehouse-size').value;
    const edition = document.getElementById('edition').value;
    const frequency = document.getElementById('frequency').value;
    
    // Update range display
    document.getElementById('records-value').textContent = `${recordsMillions}M`;
    
    // Calculate scaling factors
    const recordsRatio = recordsMillions / BASE_METRICS.records;
    const warehouseMultiplier = WAREHOUSE_MULTIPLIERS[warehouseSize];
    const creditRate = CREDIT_RATES[edition];
    
    // Calculate scaled metrics
    const scaledCredits = BASE_METRICS.credits * recordsRatio * warehouseMultiplier;
    const scaledExecutionTime = BASE_METRICS.executionMinutes * recordsRatio / Math.sqrt(warehouseMultiplier);
    const costPerRun = scaledCredits * creditRate;
    
    // Calculate frequency costs
    const frequencyMultiplier = getFrequencyMultiplier(frequency);
    const monthlyCost = costPerRun * frequencyMultiplier;
    const annualCost = monthlyCost * 12;
    
    // Efficiency metrics
    const creditsPerMillion = scaledCredits / recordsMillions;
    const costPerMillion = costPerRun / recordsMillions;
    
    // Display results
    displayResults({
        records: recordsMillions,
        credits: scaledCredits,
        executionTime: scaledExecutionTime,
        costPerRun,
        monthlyCost,
        annualCost,
        creditsPerMillion,
        costPerMillion,
        warehouseSize,
        edition,
        frequency
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
    
    if (!resultsDiv) {
        console.error('Results div not found!');
        return;
    }
    
    resultsDiv.innerHTML = `
        <div class="result-item">
            <span class="result-label">Records</span>
            <span class="result-value">${results.records}M</span>
        </div>
        <div class="result-item">
            <span class="result-label">Credits</span>
            <span class="result-value">${results.credits.toFixed(6)}</span>
        </div>
        <div class="result-item">
            <span class="result-label">Runtime</span>
            <span class="result-value">${results.executionTime.toFixed(1)}m</span>
        </div>
        <div class="result-item highlight">
            <span class="result-label">Cost per Run</span>
            <span class="result-value">$${results.costPerRun.toFixed(4)}</span>
        </div>
        <div class="result-item highlight">
            <span class="result-label">Monthly (${results.frequency})</span>
            <span class="result-value">$${results.monthlyCost.toFixed(2)}</span>
        </div>
        <div class="result-item highlight">
            <span class="result-label">Annual</span>
            <span class="result-value">$${results.annualCost.toFixed(2)}</span>
        </div>
        <div class="result-item">
            <span class="result-label">Credits/Million</span>
            <span class="result-value">${results.creditsPerMillion.toFixed(6)}</span>
        </div>
        <div class="result-item">
            <span class="result-label">Cost/Million</span>
            <span class="result-value">$${results.costPerMillion.toFixed(4)}</span>
        </div>
    `;
}

// Initialize calculator
document.addEventListener('DOMContentLoaded', function() {
    // Run initial calculation
    calculateCosts();
    
    // Add event listeners
    document.getElementById('records').addEventListener('input', calculateCosts);
    document.getElementById('warehouse-size').addEventListener('change', calculateCosts);
    document.getElementById('edition').addEventListener('change', calculateCosts);
    document.getElementById('frequency').addEventListener('change', calculateCosts);
    
    // Update slider value display in real-time
    document.getElementById('records').addEventListener('input', function() {
        const value = parseFloat(this.value);
        document.getElementById('records-value').textContent = `${value}M`;
    });
});
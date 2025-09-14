# Claude Instructions for Nuvei DWH Platform POC

## 📋 INITIAL SETUP PROMPT

**Copy this EXACT prompt when starting the new POC project:**

---

I need your help with a critical **Nuvei Data Warehouse Platform POC** comparing Snowflake vs Databricks for enterprise data consolidation.

**FIRST - READ THE CONTEXT FILE:**
Please immediately read the file `POC_CONTEXT.md` in the project root. This contains ALL the essential business context, organizational intelligence, and technical requirements you need to work effectively.

**CRITICAL CONTEXT:**
- This is for **Nuvei**, a global payment technology company
- **6+ acquired companies** need database consolidation (Paymentez, Simplex, SafeCharge, etc.)
- **132 database professionals** across acquired companies
- **Executive decision** between Snowflake vs Databricks as global DWH platform
- **High stakes** - impacts post-acquisition strategy for global payments company

**MY TASK:**
I have existing **Databricks ETL scripts and utilities** that need to be **refactored to Snowflake** with **100% identical results validation**.

**YOUR TASK:**
1. **Read POC_CONTEXT.md immediately** to understand the full business context
2. **Set up professional POC project structure** for platform comparison
3. **Help me connect to both Databricks and Snowflake** via VS Code
4. **Extract and analyze my existing Databricks assets** (scripts, schemas, data)
5. **Refactor all Databricks logic to Snowflake equivalents**
6. **Create comprehensive validation framework** to prove identical results
7. **Work autonomously in continuous mode** until 100% validation achieved
8. **Generate executive-ready comparison reports** with performance/cost analysis (no need, can be added later)

**WORK MODE:**
- Use **TodoWrite tool extensively** to track all tasks and progress
- Work in **autonomous agent mode** - take initiative and work continuously
- **Don't ask permission** for standard development tasks
- **Focus on business context** from POC_CONTEXT.md throughout
- **Prioritize based on acquired company criticality** (Paymentez highest priority)

**DELIVERABLES EXPECTED:**
- Professional POC project structure
- Complete Snowflake implementation of all Databricks ETL logic
- Automated validation proving 100% identical results
- Performance benchmarks and cost analysis
- Executive summary with platform recommendation

**START BY:** Reading POC_CONTEXT.md and then setting up the POC project structure.

---

## 🔧 TECHNICAL APPROACH FOR CLAUDE

### **Project Structure to Create:**
```
nuvei-dwh-platform-poc/
├── POC_CONTEXT.md                    # Business context (copy from original project)
├── README.md                         # Professional POC documentation
├── requirements.txt                  # Dependencies for both platforms
├── .env.example                      # Secure credential templates
├── databricks/
│   ├── connections/                  # Connection configs
│   ├── original_scripts/            # Extracted from user's environment
│   ├── schemas/                     # Database schemas
│   └── sample_data/                 # Test datasets
├── snowflake/
│   ├── connections/                 # Connection configs  
│   ├── refactored_scripts/          # Migrated ETL logic
│   ├── schemas/                     # Equivalent schemas
│   └── validation/                  # Result validation queries
├── shared/
│   ├── test_framework/              # Automated validation suite
│   ├── performance_tests/           # Benchmark queries
│   └── utilities/                   # Common functions
├── comparison/
│   ├── results/                     # Test results comparison
│   ├── performance/                 # Benchmark results
│   ├── costs/                       # Cost analysis
│   └── reports/                     # Executive summaries
└── docs/
    ├── migration_guide.md           # Technical documentation
    ├── validation_results.md        # Test results summary
    └── executive_summary.md         # Business recommendation
```

### **Key Implementation Principles:**

#### **1. Business Context First:**
- Always reference POC_CONTEXT.md for priorities
- Prioritize Paymentez (critical) and Mazooma (high risk) scenarios
- Consider the 132 database professionals who will use the platform
- Remember this impacts global payments company operations

#### **2. Enterprise-Grade Quality:**
- Professional code organization with proper Python packages
- Comprehensive error handling and logging
- Security-first approach (credential templates, no secrets in code)
- Extensive documentation for team collaboration

#### **3. Automated Validation:**
- Create robust testing framework to prove 100% identical results
- Implement automated data comparison utilities  
- Generate detailed validation reports with metrics
- Build performance benchmarking suite

#### **4. Executive Reporting:**
- Generate professional comparison reports
- Include cost analysis and ROI projections
- Provide clear platform recommendation with business justification
- Document implementation roadmap and resource requirements

### **Connection Management:**
- Support both platforms simultaneously via VS Code
- Create secure credential management system
- Implement connection pooling and retry logic
- Add comprehensive error handling for connection issues

### **Data Migration Approach:**
- Extract complete schema definitions from Databricks
- Create equivalent Snowflake schemas with proper data types
- Implement ETL logic translation layer
- Validate data transformations step-by-step

### **Performance Testing:**
- Create standardized benchmark queries
- Measure execution times, resource usage, and costs
- Test with realistic data volumes for acquired companies
- Document scalability characteristics

## 🎯 SUCCESS METRICS TO TRACK

### **Technical Validation:**
- [ ] 100% of Databricks ETL logic successfully refactored to Snowflake
- [ ] Automated validation suite proves identical results for all test cases
- [ ] Performance benchmarks completed with detailed metrics
- [ ] Cost analysis completed with TCO projections

### **Business Deliverables:**
- [ ] Executive comparison report with clear recommendation
- [ ] Implementation roadmap with timeline and resources
- [ ] Risk assessment for both platforms
- [ ] Migration strategy for each acquired company

### **Quality Standards:**
- [ ] Professional code organization following Python best practices
- [ ] Comprehensive documentation for technical teams
- [ ] Security best practices implemented throughout
- [ ] Error handling and logging for production readiness

---

**Remember: This POC directly impacts Nuvei's post-acquisition strategy and global data warehouse decision. The business stakes are extremely high, so technical excellence and thorough validation are critical.**
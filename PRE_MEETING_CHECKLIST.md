# 🔍 PRE-MEETING SANITY CHECK CHECKLIST
*Run this before stakeholder review to catch obvious issues*

## ⚠️ CRITICAL VERIFICATIONS (Don't Miss These!)

### 1. **BASIC COUNTS** 
- [ ] **Column Count**: Snowflake NCP_SILVER_V2 has exactly **174 columns**
- [ ] **Row Count**: Exactly **12,686,818 rows** (matches Databricks)
- [ ] **Date Range**: All data is from **2025-09-05** only (single day)
- [ ] **Unique Transactions**: 12,686,818 unique transaction_main_id values

### 2. **SCHEMA BASICS**
- [ ] **Table Exists**: `POC.PUBLIC.NCP_SILVER_V2` table exists and accessible
- [ ] **Key Columns Present**: transaction_main_id, transaction_date, transaction_type, amount_in_usd
- [ ] **Data Types Correct**: Booleans are BOOLEAN, dates are TIMESTAMP, amounts are DECIMAL
- [ ] **No Missing Columns**: All expected derived columns (status flags, boolean conversions) present

### 3. **KEY BUSINESS METRICS**
- [ ] **liability_shift_true**: Exactly **2,402,585** records (was the final fix)
- [ ] **Transaction Types**: AUTH, SALE, AUTH3D, INITAUTH3D, SETTLE, etc. all present
- [ ] **Success Results**: ~80% transactions with result_id = '1006' (success)
- [ ] **Status Flags**: Millions of records with populated status flags (not all NULL)

### 4. **DERIVED COLUMNS WORKING**
- [ ] **Status Flags**: init_status, auth_status, sale_status, auth_3d_status have values
- [ ] **Conditional Copies**: is_sale_3d_auth_3d populated for AUTH3D transactions only
- [ ] **Boolean Conversions**: is_approved, is_declined, is_3d have TRUE/FALSE values (not strings)
- [ ] **3D Secure Analysis**: is_successful_challenge, is_successful_exemption have values

### 5. **DATA QUALITY BASICS**
- [ ] **No Test Clients**: Zero records with test multi-client names
- [ ] **No NULL Keys**: transaction_main_id and transaction_date never NULL  
- [ ] **Amount Sanity**: amount_in_usd values reasonable (not all zero, not negative)
- [ ] **Date Consistency**: All transaction_date values are 2025-09-05

## 🚨 RED FLAGS TO WATCH FOR

### Schema Issues:
- ❌ **Wrong Column Count**: If not 174 columns, schema mismatch
- ❌ **Missing Derived Columns**: Status flags, boolean conversions not present
- ❌ **Wrong Data Types**: Boolean fields showing as VARCHAR instead of BOOLEAN

### Data Issues:  
- ❌ **Wrong Row Count**: If not 12,686,818 rows, data filtering problem
- ❌ **Wrong Date Range**: Multiple dates or wrong date = source data issue
- ❌ **All NULLs in Derived**: Status flags all NULL = business logic broken
- ❌ **liability_shift = 0**: Should be 2,402,585 true values

### Business Logic Issues:
- ❌ **No Status Flags**: All status flag columns empty = ETL logic failed
- ❌ **No Boolean Conversion**: String values in boolean fields = conversion failed  
- ❌ **Zero Conditional Copies**: Should have ~1.57M auth3d conditional copies

## 📋 QUICK EXECUTION CHECKLIST

**Before Meeting:**
1. [ ] Run `sanity_check_comprehensive.sql` and review all results
2. [ ] Verify all checkboxes above are ✅
3. [ ] Compare a few sample records visually with Databricks data
4. [ ] Check validation results2.txt shows Level 7 liability_shift: 2,402,585

**During Meeting:**  
1. [ ] Show column count first (174 columns)
2. [ ] Show row count match (12,686,818 exact match)
3. [ ] Highlight liability_shift fix (final breakthrough)
4. [ ] Walk through validation levels 1-10 results
5. [ ] Show sample transaction records side-by-side

**Questions to Prepare For:**
- "Are you sure we have all the columns?" → **174 columns confirmed**
- "Is the row count exactly right?" → **12,686,818 exact match**  
- "Are all derived columns working?" → **All 6 status flags + boolean conversions working**
- "What about liability_shift?" → **2,402,585 true values - FIXED!**

## 🎯 SUCCESS CRITERIA CONFIRMED
- ✅ **10/10 validation levels** passing (perfect/near-perfect)
- ✅ **100% schema coverage** (174 columns)
- ✅ **100% business logic parity** (complex derived columns working)
- ✅ **Exact data volume match** (12.6M+ records)
- ✅ **All boolean conversions perfect** (13/13 fields including liability_shift)

*This checklist prevents the "obvious things we missed" scenario!*
# SQL Translation Test Cases

This document outlines the test cases for SQL translation between different database platforms.

## Basic Translation Cases

1. **Data Type Mappings**
   - VARCHAR/STRING conversions
   - DECIMAL/NUMERIC precision handling
   - DATE/TIMESTAMP/DATETIME variations
   - BOOLEAN representations

2. **Function Translations**
   - Date functions (GETDATE, CURRENT_DATE, etc.)
   - String functions (SUBSTRING, CONCAT, etc.)
   - Numeric functions (ROUND, CEIL, etc.)
   - NULL handling (NVL, COALESCE, IFNULL)

3. **Syntax Variations**
   - LIMIT vs TOP
   - Date literals
   - String concatenation
   - CASE statements
   - JOIN syntax

## Advanced Translation Cases

1. **Window Functions**
   - ROW_NUMBER()
   - RANK() and DENSE_RANK()
   - LAG() and LEAD()
   - FIRST_VALUE and LAST_VALUE
   - Custom window frames

2. **Common Table Expressions (CTEs)**
   - Basic CTEs
   - Recursive CTEs
   - Multiple CTEs
   - CTE naming conventions

3. **Platform-Specific Features**
   - Redshift: DISTKEY and SORTKEY
   - Databricks: DELTA LAKE and CLUSTER BY
   - Snowflake: Warehouse settings
   - BigQuery: Partitioning and clustering
   - PostgreSQL: Inheritance
   - MySQL: Storage engines
   - SQL Server: Temporal tables

## Test Scenarios

### Scenario 1: Customer Analytics
File: `customer_analytics.sql`
Tests:
- Basic table creation
- Column type mappings
- Simple joins and filters
- Date calculations
- Platform-specific optimizations

### Scenario 2: Customer Segmentation
File: `customer_segmentation.sql`
Tests:
- Complex CTEs
- Window functions
- Conditional logic
- Aggregations
- Date manipulations
- NULL handling

## Translation Validation

For each test case:
1. Verify syntax compatibility
2. Check data type mappings
3. Validate function translations
4. Confirm query results
5. Review performance implications

## Common Translation Challenges

1. **Data Type Precision**
   - Decimal/numeric precision differences
   - Date/time format variations
   - String length limitations

2. **Function Compatibility**
   - Different function names
   - Parameter order variations
   - Return type differences

3. **Syntax Differences**
   - Table creation options
   - Index definitions
   - Partitioning syntax
   - Transaction handling

4. **Performance Features**
   - Distribution keys
   - Clustering keys
   - Partitioning strategies
   - Index types

## Test Data Requirements

1. **Volume**
   - Minimum 1000 customers
   - 10000+ events
   - 1000+ products
   - 5000+ orders

2. **Variety**
   - Different customer segments
   - Various event types
   - Multiple product categories
   - Diverse order patterns

3. **Edge Cases**
   - NULL values
   - Boundary dates
   - Special characters
   - Extreme values

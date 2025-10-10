-- Common schema definition for test data
-- This represents the base tables that will be used across all platforms

-- Customer table
CREATE TABLE raw_customers (
    customer_id varchar(50),
    name varchar(100),
    email varchar(100),
    signup_date date,
    country varchar(50),
    segment varchar(20)
);

-- Events table
CREATE TABLE raw_events (
    event_id varchar(50),
    customer_id varchar(50),
    event_type varchar(20),
    event_date date,
    revenue decimal(10,2),
    source varchar(50),
    device varchar(20),
    location varchar(100)
);

-- Products table
CREATE TABLE raw_products (
    product_id varchar(50),
    name varchar(100),
    category varchar(50),
    price decimal(10,2),
    inventory int,
    supplier_id varchar(50)
);

-- Orders table
CREATE TABLE raw_orders (
    order_id varchar(50),
    customer_id varchar(50),
    order_date date,
    status varchar(20),
    total_amount decimal(10,2),
    payment_method varchar(20),
    shipping_address varchar(200)
);

-- Order Items table
CREATE TABLE raw_order_items (
    order_id varchar(50),
    product_id varchar(50),
    quantity int,
    unit_price decimal(10,2),
    discount decimal(5,2)
);

-- This schema provides a foundation for testing various SQL features:
-- 1. Joins between multiple tables
-- 2. Date manipulations
-- 3. Aggregations and window functions
-- 4. String operations
-- 5. Numeric calculations
-- 6. Different data types
-- 7. NULL handling

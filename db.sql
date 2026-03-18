CREATE DATABASE db;
USE db;

-- 1. Source Table
CREATE TABLE IF NOT EXISTS source_sales (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    updated_at DATETIME
);

-- 2. Staging Table (Includes ETL metadata + Source columns)
CREATE TABLE IF NOT EXISTS stg_sales (
    id INT,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    updated_at DATETIME
);

-- 3. Operational Table (Target)
CREATE TABLE IF NOT EXISTS op_sales (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    updated_at DATETIME
);

-- 4. Control Table (Metadata)
CREATE TABLE IF NOT EXISTS control_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(50),
    source_table VARCHAR(100),
    staging_table VARCHAR(100),
    operational_table VARCHAR(100),
    ingestion_type VARCHAR(50),
    last_loaded DATETIME,
    primary_key_col VARCHAR(50) -- e.g., 'id'
);

-- 5. Logging Table
CREATE TABLE IF NOT EXISTS logging_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert VARCHAR(5), -- 'Yes' or 'No'
    last_loaded DATETIME,
    stg_tablename VARCHAR(100),
    row_loaded INT,
    status VARCHAR(20), -- 'Success' or 'Failed'
    schema_change VARCHAR(5) -- 'Yes' or 'No'
);

DELETE FROM control_table;

INSERT INTO control_table 
(source, source_table, staging_table, operational_table, ingestion_type, last_loaded, primary_key_col)
VALUES 
('mysql', 'source_sales', 'stg_sales', 'op_sales', 'incremental', '2026-03-18 08:00:00', 'id');

-- Insert initial data into source_sales
INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(1, 'Laptop', 40000.00, '2026-03-18 09:01:00'),
(2, 'Keyboard', 900.00,'2026-03-18 09:19:00'),
(3, 'Power Bank', 1500.00,'2026-03-18 09:40:00');
INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(4, 'Smartphone', 20000.00, '2026-03-18 10:01:00'),
(5, 'Mouse', 800.00, now()),
(6, 'Monitor', 15000.00, now()),
(7, 'Tablet', 12000.00, now());

INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(10, 'USB Cable', 300.00, NOW()),
(11, 'Speaker', 2500.00, NOW()),
(12, 'Router', 3500.00, NOW()),
(13, 'SSD', 5000.00, NOW()),
(14, 'Projector', 30000.50, NOW()),
(15, 'Projector', 30000.50, NOW()),
(16, 'TV', 60000, NOW()),
(17, 'AC', 50000, NOW()),
(18, 'Fridge', 45000, NOW()),
(19, 'Washing Machine', 30000, NOW()),
(20, 'Microwave', 15000, NOW());


-- schema evolution

ALTER TABLE source_sales MODIFY COLUMN amount int;
INSERT INTO source_sales (id, name, amount, updated_at) VALUES
(21, 'Scanner', 12000.50, NOW());

ALTER TABLE source_sales MODIFY COLUMN amount decimal(10,2);
-- Add category column to source_sales
ALTER TABLE source_sales ADD COLUMN category VARCHAR(50);
UPDATE source_sales SET category = 'Electronics', updated_at = NOW() WHERE id = 1;
UPDATE source_sales SET category = 'Electronics', updated_at = NOW() WHERE id = 2;


-- Final select to verify all data
SELECT * FROM source_sales;
SELECT * FROM stg_sales;
SELECT * FROM op_sales;
SELECT * FROM logging_table;
select * from control_table;

-- Drop category column from all tables (maintaining consistency)
ALTER TABLE source_sales DROP COLUMN category;
ALTER TABLE stg_sales DROP COLUMN category;
ALTER TABLE op_sales DROP COLUMN category;

truncate source_sales;
truncate stg_sales;
truncate logging_table;
truncate op_sales;
truncate control_table;

-- show databases;
-- -- CREATE DATABASE vegetable_db;
-- show databases;
-- use vegetable_db;
-- show tables;

-- -- drop TABLE wholesale_table;
-- -- drop TABLE loss_table;
-- -- drop TABLE Item_table;
-- -- drop TABLE sales_table;

-- -- Create TABLE2: item_table
-- CREATE TABLE item_table (
--     Item_Code BIGINT PRIMARY KEY,
--     Item_Name VARCHAR(255),
--     Category_Code INT NOT NULL,
--     Category_Name VARCHAR(255)
-- );


-- -- Create TABLE1: sales_table
-- CREATE TABLE sales_table (
--     sale_id BIGINT PRIMARY KEY AUTO_INCREMENT,
--     Date DATE,
--     Time TIME,
--     Item_Code BIGINT NOT NULL,
--     Quantity_Sold FLOAT,
--     Unit_Selling_Price FLOAT,
--     Sale_or_Return VARCHAR(255),
--     Discount VARCHAR(10),
--     FOREIGN KEY (Item_Code) REFERENCES item_table(Item_Code)
-- );

-- -- Create TABLE3: whole_sale
-- CREATE TABLE whole_sale (
--     Date DATE,
--     Item_Code BIGINT,
--     Wholesale_Price FLOAT,
--     PRIMARY KEY (Date, Item_Code),
--     FOREIGN KEY (Item_Code) REFERENCES item_table(Item_Code)
-- );

-- -- Create TABLE4: loss
-- CREATE TABLE loss (
--     Item_Code BIGINT PRIMARY KEY,
--     Item_Name VARCHAR(255),
--     Loss_Rate FLOAT
-- );

-- show tables;


-- LOAD DATA  INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Item_category.csv"
-- INTO TABLE item_table
-- FIELDS TERMINATED BY ','
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES;

-- LOAD DATA  INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\whole_sale.csv"
-- INTO TABLE whole_sale
-- FIELDS TERMINATED BY ','
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES;

-- LOAD DATA  INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\loss_rate.csv"
-- INTO TABLE loss
-- FIELDS TERMINATED BY ','
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES;

-- -- SHOW STATUS LIKE 'Uptime';

-- LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sales.csv"
-- INTO TABLE sales_table
-- FIELDS TERMINATED BY ','
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES
-- (Date, Time, Item_Code, Quantity_Sold, Unit_Selling_Price, Sale_or_Return, Discount);

-- -- SELECT COUNT(*) AS total_rows FROM sales_table;

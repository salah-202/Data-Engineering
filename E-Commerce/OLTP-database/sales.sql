CREATE DATABASE sales;

USE sales;

CREATE TABLE sales_data (
    product_id INT NOT NULL ,
    customer_id INT NOT NULL ,
    price INT NOT NULL ,
    quantity INT NOT NULL ,
    timestamp DATETIME NOT NULL
    );


USE sales;

LOAD DATA INFILE '/var/lib/mysql-files/oltpdata.csv'
    INTO TABLE sales_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

SELECT * FROM sales_data;

SELECT COUNT(*) FROM sales_data;

CREATE INDEX ts
ON sales_data (timestamp);


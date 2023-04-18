USE softcart;

LOAD DATA INFILE '/var/lib/mysql-files/DimDate.csv'
    INTO TABLE softcartDimDate FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/var/lib/mysql-files/DimCategory.csv'
    INTO TABLE softcartDimCategory FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/var/lib/mysql-files/DimCountry.csv'
    INTO TABLE softcartDimCountry FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA INFILE '/var/lib/mysql-files/FactSales.csv'
    INTO TABLE softcartFactSales FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


DELETE FROM softcartFactSales
WHERE countryid = 22;

DELETE FROM softcartDimCountry
WHERE countryid = 22;

/*Grouping sets in MySQL*/
SELECT country,NULL AS category,SUM(sFS.amount) AS total_sales FROM softcartFactSales sFS
JOIN softcartDimCountry sDC on sFS.countryid = sDC.countryid
GROUP BY country
UNION ALL
SELECT NULL,category,SUM(sFS.amount) AS total_sales FROM softcartFactSales sFS
JOIN  softcartDimCategory sDC2 on sFS.categoryid = sDC2.categoryid
GROUP BY category;

/*Rollup in MySQL*/
SELECT year,country,SUM(amount) AS total_sales,AVG(amount) AS average_sales FROM softcartFactSales sFS
JOIN softcartDimDate sDD on sFS.dateid = sDD.dateid
JOIN softcartDimCountry sDC on sFS.countryid = sDC.countryid
GROUP BY year, country WITH ROLLUP;

/*Create Materialized Query Table*/
CREATE TABLE MQT(
    country varchar(128) NOT NULL ,
    total_sales int NOT NULL
);
CREATE PROCEDURE refreash_MQT ()
BEGIN
   -- SQL statements and programming constructs
    TRUNCATE TABLE MQT;

   INSERT INTO MQT(country, total_sales)
    SELECT country,SUM(amount) FROM softcartFactSales
        JOIN softcartDimCountry sDC on sDC.countryid = softcartFactSales.countryid
        GROUP BY country;
END;

/*Just run the last command line*/
CREATE EVENT refresh_materialized_view_event
ON SCHEDULE EVERY 1 DAY
STARTS '2023-03-15 01:00:00'
DO
CALL refreash_MQT();










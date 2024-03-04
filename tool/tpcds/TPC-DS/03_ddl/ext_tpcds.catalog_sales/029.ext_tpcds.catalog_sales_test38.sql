CREATE EXTERNAL TABLE ext_tpcds.catalog_sales_test38 (like tpcds.catalog_sales)
LOCATION (:LOCATION)
FORMAT 'TEXT' (DELIMITER '|' NULL AS '' ESCAPE AS E'\\');


CREATE EXTERNAL TABLE ext_tpcds.web_sales_test38 (like tpcds.web_sales)
LOCATION (:LOCATION)
FORMAT 'TEXT' (DELIMITER '|' NULL AS '' ESCAPE AS E'\\');


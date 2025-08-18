create streaming table silver.sales_cleaned
(constraint valid_order_id expect (order_id is not null)on violation drop row)
as
select distinct * except(_rescued_data,ingestion_ts) from stream sales ;

-- products scd 1 

-- https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into 

-- Create a streaming table, then use AUTO CDC to populate it: 

CREATE OR REFRESH STREAMING TABLE silver.products_silver;
CREATE FLOW product_flow
AS AUTO CDC INTO silver.products_silver
FROM stream(bronze.products)
KEYS (product_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY seqNum
COLUMNS * EXCEPT (operation,seqNum,_rescued_data,ingestion_ts)
STORED AS SCD TYPE 1;

--TRACK HISTORY ON * EXCEPT (city)

-- customers scd 2

CREATE OR REFRESH STREAMING TABLE silver.customers_silver;
CREATE FLOW customers_flow
AS AUTO CDC INTO silver.customers_silver
FROM stream(bronze.customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY sequenceNum
COLUMNS * EXCEPT (operation,sequenceNum,_rescued_data,ingestion_ts)
STORED AS SCD TYPE 2;
 
-- sales
create streaming table if not exists sales as 
select *, current_timestamp() as ingestion_ts from stream read_files("/Volumes/pipeline_ecom/default/input_file/sales/",format=>"csv");

-- 
create streaming table if not exists products as 
select *, current_timestamp() as ingestion_ts from stream read_files("/Volumes/pipeline_ecom/default/input_file/products/",format=>"csv");

create streaming table if not exists customers as 
select *, current_timestamp() as ingestion_ts from stream read_files("/Volumes/pipeline_ecom/default/input_file/customers/",format=>"csv");
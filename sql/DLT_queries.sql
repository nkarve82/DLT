-- Databricks notebook source
-- MAGIC %python
-- MAGIC # spark.read.csv("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/MyData.csv").collect()
-- MAGIC 
-- MAGIC spark.read.csv("abfss://devc-dlt-demo@dvcpmdpadbadls.dfs.core.windows.net/RLS_Nvarchar_test.csv").collect()
-- MAGIC 
-- MAGIC 
-- MAGIC # https://dvcpmdpadbadls.dfs.core.windows.net/devc-dlt-demo/csvData.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.mount(
-- MAGIC source = "wasbs://devc-dlt-demo@dvcpmdpadbadls.blob.core.windows.net",
-- MAGIC mount_point = "/mnt/devc-dlt-demo2",
-- MAGIC extra_configs = {"fs.azure.account.key.dvcpmdpadbadls.blob.core.windows.net":dbutils.secrets.get(scope = "dlt0demo-secret-scope", key = "dlt-demo-secret-key")})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.cp("/databricks-datasets/retail-org/", "/mnt/devc-dlt-demo/retail-org/", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC configs = {
-- MAGIC   "fs.azure.account.auth.type": "CustomAccessToken",
-- MAGIC   "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
-- MAGIC }
-- MAGIC 
-- MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "abfss://devc-dlt-demo@dvcpmdpadbadls.dfs.core.windows.net/",
-- MAGIC   mount_point = "/mnt/devc-dlt-demo",
-- MAGIC   extra_configs = configs)

-- COMMAND ----------

SELECT * , current_timestamp() as UpdatedDate 
FROM csv.`abfss://devc-dlt-demo@dvcpmdpadbadls.dfs.core.windows.net/RLS_Nvarchar_test.csv` 
--options (header "true")
--option ( header = true);

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC display(dbutils.fs.ls("/databricks-datasets/retail-org/customers/"))

-- COMMAND ----------

DROP TABLE IF EXISTS customers2;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlQuery = """SELECT *
-- MAGIC                 FROM customers2
-- MAGIC                limit 10 """
-- MAGIC 
-- MAGIC df = spark.sql(sqlQuery)
-- MAGIC 
-- MAGIC df.repartition(1).write.format('com.databricks.spark.csv').save("/databricks-datasets/retail-org/customers/mydata.csv",header = 'true')

-- COMMAND ----------

--28813

DROP TABLE IF EXISTS customers4;
 
CREATE TABLE customers4
USING DELTA
LOCATION '/mnt/devc-dlt-demo2/stream-retail-org/customers/';

SELECT * FROM customers4
order by cast(customer_id as int)

-- COMMAND ----------

 DROP TABLE IF EXISTS customers2;

CREATE TABLE  customers2
USING csv
OPTIONS (
  path "/mnt/devc-dlt-demo2/retail-org/customers/"
  ,header true
);

SELECT * FROM customers2
order by cast(customer_id as int)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.readStream\
-- MAGIC   .format("cloudFiles")\
-- MAGIC   .option("cloudFiles.format", "csv")\
-- MAGIC   .option("cloudFiles.schemaLocation", "/mnt/devc-dlt-demo2/checkpoint/")\
-- MAGIC   .load("/mnt/devc-dlt-demo2/retail-org/customers/")\
-- MAGIC   .writeStream.option("checkpointLocation", "/mnt/devc-dlt-demo2/checkpoint/")\
-- MAGIC   .start("/mnt/devc-dlt-demo2/stream-retail-org/customers/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC spark.readStream.format('cloudFiles').option('cloudFiles.format', 'csv'
-- MAGIC         ).option('cloudFiles.schemaLocation',
-- MAGIC                  '/mnt/devc-dlt-demo2/checkpoint/'
-- MAGIC                  ).load('/mnt/devc-dlt-demo2/retail-org/customers/'
-- MAGIC                         ).writeStream.option('checkpointLocation',
-- MAGIC         '/mnt/devc-dlt-demo2/checkpoint/'
-- MAGIC         ).start('/mnt/devc-dlt-demo2/stream-retail-org/customers/')

-- COMMAND ----------


df = spark.read.format("csv").option("inferSchema", True).option("header", True).load("/mnt/devc-dlt-demo/retail-org/customers/")
dataset_schema = df.schema

df_out = df.writeStream
  .trigger(once=True)
  .format("csv")
  .outputMode("append")
  .option("checkpointLocation", "/mnt/devc-dlt-demo/checkpointcsv")
  .start("/mnt/devc-dlt-demo/autoloadercsv")



-- COMMAND ----------

CREATE TABLE streamExmpl
AS 
SELECT * 
FROM STREAM(customers);

-- COMMAND ----------

CREATE OR REPLACE TABLE customers
AS SELECT * , current_timestamp() as UpdatedDate 
FROM csv.`/databricks-datasets/retail-org/customers/`;

select * from customers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC def get_dir_content(ls_path):
-- MAGIC   dir_paths = dbutils.fs.ls(ls_path)
-- MAGIC   subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
-- MAGIC   flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
-- MAGIC   return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths
-- MAGIC     
-- MAGIC 
-- MAGIC paths = get_dir_content('/databricks-datasets/retail-org/customers/')
-- MAGIC [print(p) for p in paths]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC f = open("/dbfs/databricks-datasets/retail-org/sales_orders/part-00000-tid-1771549084454148016-e2275afd-a5bb-40ed-b044-1774c0fdab2b-105592-1-c000.json", "r")
-- MAGIC print(f.read())

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC display(dbutils.fs.ls("dbfs:/databricks-datasets/retail-org/customers/"))

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC display(dbutils.fs.ls("dbfs:/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC f = open("/dbfs/databricks-datasets/retail-org/customers/customers.csv", "r")
-- MAGIC print(f.read())

-- COMMAND ----------

select * 
from DLT_Demo_Wiki.event_log

-- COMMAND ----------

select * 
from DLT_Demo_Wiki.clickstream_raw

-- COMMAND ----------

select  updateddate2, date_format(updateddate2, 'HH:mm:ss') updatetime , date_format( current_timestamp(), 'HH:mm:ss') as now, * 
from DLT_Demo_RetailSales.sales_orders_raw
order by updateddate2 desc

-- COMMAND ----------

select  date_format(updateddate2, 'HH:mm:ss') updateddate2 , date_format( current_timestamp(), 'HH:mm:ss') as now, * 
from DLT_Demo_RetailSales.sales_orders_raw
order by updateddate2 desc

-- COMMAND ----------

select updateddate2, current_timestamp() as now, * 
from DLT_Demo_RetailSales.sales_orders_raw
order by updateddate2 desc

-- COMMAND ----------

select updateddate3, * 
from DLT_Demo_RetailSales.sales_orders_streamed
order by updateddate3 desc

-- COMMAND ----------

select updateddate3, * 
from DLT_Demo_RetailSales.sales_orders_streamed
order by updateddate3 desc

-- COMMAND ----------

select count(1)
from DLT_Demo_RetailSales.customers

-- COMMAND ----------

select * 
from DLT_Demo_RetailSales.sales_order_in_la

-- COMMAND ----------

select * 
from DLT_Demo_RetailSales.sales_orders_raw

-- COMMAND ----------

select * 
from deltalive_wiki_demo.top_spark_referers

-- COMMAND ----------

select * 
from DLT_Demo_Wiki.top_spark_referers 

-- COMMAND ----------

select * 
from deltalive_wiki_demo.top_pages

-- COMMAND ----------

select count(1) ,'full'
from deltalive_wiki_demo.clickstream_clean
union
select count(1) , 'click count more than 1000'
from deltalive_wiki_demo.clickstream_clean
where click_count >5000

-- COMMAND ----------

select count(1) ,'Clean'
from deltalive_wiki_demo.clickstream_clean
union
select count(1) , 'Bad'
from deltalive_wiki_demo.clickstream_bad


-- COMMAND ----------

select count(1) ,'Clean'
from deltalive_test.clickstream_clean
union
select count(1) , 'Bad'
from deltalive_test.clickstream_bad

-- COMMAND ----------

select 1 
from  deltalive_wiki_demo.clickstream_clean

union 
select 2
from  deltalive_wiki_demo.clickstream_clean


-- COMMAND ----------

create or replace temp view test
as
select 1 , 'text1'
from  deltalive_wiki_demo.clickstream_clean
union
select 2 , 'text2'
from  deltalive_wiki_demo.clickstream_clean
union
select NULL , NULL
from  deltalive_wiki_demo.clickstream_clean
union
Select NULL , ''
from  deltalive_wiki_demo.clickstream_clean
union
SELECT 3, 'null'
from  deltalive_wiki_demo.clickstream_clean

-- COMMAND ----------

select * 
from test

-- COMMAND ----------

select * 
from test
where text1 IS NULL or text1 = 'null'

-- COMMAND ----------

select * 
from test
where text1 IS NULL 

-- COMMAND ----------

select *
from deltalive_wiki_demo.clickstream_bad
where current_page_id is NULL

-- COMMAND ----------

select count(1) 
from deltalive_wiki_demo.clickstream_raw

-- COMMAND ----------

--- An example Delta Live Tables pipeline that ingests wikipedia click stream data and builds some simple summary tables.

CREATE LIVE TABLE clickstream_raw
COMMENT "The raw wikipedia click stream dataset, ingested from /databricks-datasets."
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`

-- COMMAND ----------

CREATE LIVE TABLE clickstream_clean(
  CONSTRAINT valid_current_page EXPECT (current_page_id IS NOT NULL and current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Wikipedia clickstream dataset with cleaned-up datatypes / column names and quality expectations."
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  CAST (curr_id AS INT) AS current_page_id,
  curr_title AS current_page_title,
  CAST(n AS INT) AS click_count,
  CAST (prev_id AS INT) AS previous_page_id,
  prev_title AS previous_page_title
FROM live.clickstream_raw

-- COMMAND ----------

CREATE LIVE TABLE top_spark_referers
COMMENT "A table of the most common pages that link to the Apache Spark page."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  previous_page_title as referrer,
  click_count
FROM live.clickstream_clean
WHERE current_page_title = 'Apache_Spark'
ORDER BY click_count DESC
LIMIT 10

-- COMMAND ----------

CREATE LIVE TABLE top_pages
COMMENT "A list of the top 50 pages by number of clicks."
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  current_page_title,
  SUM(click_count) as total_clicks
FROM live.clickstream_clean
GROUP BY current_page_title
ORDER BY 2 DESC
LIMIT 50

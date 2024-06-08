-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Overview
-- MAGIC
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

DROP TABLE user_profile_table;
DROP TABLE music_data_table;

-- COMMAND ----------

CREATE TABLE user_profile_table
USING CSV
OPTIONS (
  path 'dbfs:/FileStore/tables/userid_profile_tsv-1.gz',
  header 'true',
  inferSchema 'false',
  delimiter '\t',
  compression 'gzip'
);


-- COMMAND ----------

CREATE TABLE music_data_table
USING CSV
OPTIONS (
  path 'dbfs:/FileStore/tables/userid_timestamp_artid_artname_traid_traname_tsv.gz',
  header 'false',
  inferSchema 'false',
  delimiter '\t',
  compression 'gzip'
);

-- COMMAND ----------

SELECT * FROM user_profile_table;


-- COMMAND ----------

SELECT * FROM music_data_table;



-- COMMAND ----------


-- Add three new columns
ALTER TABLE music_data_table
ADD COLUMN Year INT,
ADD COLUMN Month INT,
ADD COLUMN Day INT;

-- Update the new columns with values extracted from the existing column
SELECT 
    _c0,
    _c1,
    _c2,
    _c3,
    _c4,
    YEAR(CAST(_c1 AS TIMESTAMP)) AS Year,
    MONTH(CAST(_c1 AS TIMESTAMP)) AS Month,
    DAY(CAST(_c1 AS TIMESTAMP)) AS Day
FROM
    music_data_table;


-- COMMAND ----------


/* Query the created temp table in a SQL cell */

select * from `userid_profile_tsv-3_gz`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
-- MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
-- MAGIC # To do so, choose your table name and uncomment the bottom line.
-- MAGIC
-- MAGIC permanent_table_name = "userid_profile_tsv-3_gz"
-- MAGIC
-- MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)

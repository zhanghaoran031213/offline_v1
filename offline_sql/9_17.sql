create database bigdata_offline_v1_ws;

CREATE EXTERNAL TABLE IF NOT EXISTS ods_activity_info(id STRING,
 activity_name STRING,
 activity_type STRING,

  activity_desc STRING,
  start_time STRING,
   end_time STRING, create_time STRING, operate_time STRING) PARTITIONED BY (ds STRING) LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/ods_activity_info/' TBLPROPERTIES ('parquet.compress' = 'SNAPPY', 'external.table.purge' = 'true');



drop table bigdata_offline_v1_ws.ods_activity_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_activity_info(id STRING,
 activity_name STRING,
  activity_type STRING,
   activity_desc STRING,
    start_time STRING,
    end_time STRING,
    create_time STRING,
    operate_time STRING)
    PARTITIONED BY (dt STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/ods_activity_info/'
    TBLPROPERTIES ('parquet.compress' = 'SNAPPY', 'external.table.purge' = 'true');

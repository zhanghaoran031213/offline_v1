-- 1. 配置PostgreSQL CDC源表（用户表）
CREATE TABLE pg_user_source (
    id INT,
    name STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED  -- 主键用于CDC的行级定位
) WITH (
      'connector' = 'postgres-cdc',
      'hostname' = 'localhost',
      'port' = '5432',
      'username' = 'postgres',
      'password' = 'postgres',
      'database-name' = 'pg_cdc_db',  -- 数据库名
      'table-name' = 'public.pg_user',  -- 表名（schema.表名）
      'decoding.plugin.name' = 'pgoutput'  -- PostgreSQL 10+ 推荐使用pgoutput插件
      );

-- 2. 配置PostgreSQL CDC源表（订单表）
CREATE TABLE pg_order_source (
     order_id INT,
     user_id INT,
     amount DECIMAL(10,2),
     order_time TIMESTAMP(3),
     PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'postgres-cdc',
      'hostname' = 'localhost',
      'port' = '5432',
      'username' = 'postgres',
      'password' = 'postgres',
      'database-name' = 'pg_cdc_db',
      'table-name' = 'public.pg_order',
      'decoding.plugin.name' = 'pgoutput'
      );

-- 3. 配置SQL Server CDC源表（产品表）
CREATE TABLE mssql_product_source (
  product_id INT,
  product_name STRING,
  price DECIMAL(10,2),
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
      'connector' = 'sqlserver-cdc',
      'hostname' = 'localhost',
      'port' = '1433',
      'username' = 'SA',
      'password' = 'YourPassword',  -- 替换为实际SQL Server密码
      'database-name' = 'mssql_cdc_db',
      'schema-name' = 'dbo',  -- SQL Server的schema名
      'table-name' = 'mssql_product'
      );

-- 4. 配置Kafka Sink（接收PostgreSQL数据）
CREATE TABLE kafka_pg_sink (
    data STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'pg_cdc_topic',  -- 目标Kafka主题
      'properties.bootstrap.servers' = 'localhost:9092',  -- Kafka地址
      'format' = 'raw',  -- 原始字符串格式（JSON直接写入）
      'sink.partitioner' = 'round-robin'  -- 分区策略（可选）
      );

-- 5. 配置Kafka Sink（接收SQL Server数据）
CREATE TABLE kafka_mssql_sink (
    data STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'mssql_cdc_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'format' = 'raw'
      );

-- 6. 同步PostgreSQL用户表到Kafka（修正JSON生成方式）
INSERT INTO kafka_pg_sink
SELECT to_json(ROW(id, name, create_time))  -- 使用ROW构造JSON对象
FROM pg_user_source;

-- 7. 同步PostgreSQL订单表到Kafka
INSERT INTO kafka_pg_sink
SELECT to_json(ROW(order_id, user_id, amount, order_time))
FROM pg_order_source;

-- 8. 同步SQL Server产品表到Kafka
INSERT INTO kafka_mssql_sink
SELECT to_json(ROW(product_id, product_name, price))
FROM mssql_product_source;
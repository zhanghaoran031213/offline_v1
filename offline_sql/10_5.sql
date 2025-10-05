-- 创建表1：用户表
CREATE TABLE pg_user (
     id SERIAL PRIMARY KEY,
     name VARCHAR(50),
     create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 创建表2：订单表
CREATE TABLE pg_order (
  order_id SERIAL PRIMARY KEY,
  user_id INT,
  amount DECIMAL(10,2),
  order_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER SYSTEM SET wal_level = minimal;
SELECT pg_reload_conf();
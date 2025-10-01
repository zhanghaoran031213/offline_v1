RESET;
SET fs.s3a.access.key=minioadmin;
SET fs.s3a.secret.key=minioadmin;
SET fs.s3a.endpoint=http://192.168.200.32:19000;  -- 修正为 MinIO API 端口（19000）
SET fs.s3a.connection.ssl.enabled=false;

-- 2. 删除旧表（避免冲突）
DROP TABLE IF EXISTS order_info;

-- 3. 重新建表：修正 LOCATION 为 MinIO 实际桶名和基目录
CREATE EXTERNAL TABLE IF NOT EXISTS order_info (
   id STRING COMMENT '订单ID',
   consignee STRING COMMENT '收货人',
   consignee_tel STRING COMMENT '收货人电话',
   final_total_amount DOUBLE COMMENT '最终支付金额',
   order_status STRING COMMENT '订单状态（如1001=待付款）',
   user_id STRING COMMENT '用户ID',
   delivery_address STRING COMMENT '配送地址',
   order_comment STRING COMMENT '订单备注',
   out_trade_no STRING COMMENT '外部交易号',
   trade_body STRING COMMENT '商品描述',
   create_time STRING COMMENT '创建时间（格式：MM/dd/yyyy HH:mm:ss）',
   operate_time STRING COMMENT '操作时间',
   expire_time STRING COMMENT '过期时间',
   tracking_no STRING COMMENT '物流单号（可为空）',
   parent_order_id STRING COMMENT '父订单ID（可为空）',
   img_url STRING COMMENT '商品图片URL',
   province_id STRING COMMENT '省份ID',
   benefit_reduce_amount DOUBLE COMMENT '优惠减免金额',
   original_total_amount DOUBLE COMMENT '原始总金额',
   feight_fee DOUBLE COMMENT '运费',
   yys STRING COMMENT '运营商（电信/移动/联通，可为空）'
)
    PARTITIONED BY (ds STRING COMMENT '时间分区，格式：yyyyMMdd')  -- 保留 ds 分区
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = "|",
        "quoteChar" = "\"",
        "escapeChar" = "\\"
        )
    STORED AS TEXTFILE
-- 修正 LOCATION：桶名用“订单信息”（中文），基目录为 order_info/（分区目录在其下）
    LOCATION 's3a://orderinfo/order_info/';

ALTER TABLE order_info
    ADD PARTITION (ds='20250925')
-- 路径需与 MinIO 中实际路径完全一致（中文桶名+分区目录）
        LOCATION 's3a://orderinfo/order_info/';


SHOW PARTITIONS order_info;


SELECT id, consignee, final_total_amount
FROM order_info
WHERE ds='20250925'
    LIMIT 10;
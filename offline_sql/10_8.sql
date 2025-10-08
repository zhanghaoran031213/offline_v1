DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_sku_sales_top10;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_sku_sales_top10 (
    `dt` STRING COMMENT '统计日期',
    `sku_id` STRING COMMENT '商品ID',
    `sku_name` STRING COMMENT '商品名称',
    `category1_name` STRING COMMENT '一级品类',
    `tm_name` STRING COMMENT '品牌',
    `sales_amount` DECIMAL(38,18) COMMENT '销售额',
    `sales_rank` INT COMMENT '排名'
) COMMENT '商品销售TOP10'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v2_ws/ads/ads_sku_sales_top10';

INSERT OVERWRITE TABLE bigdata_offline_v2_ws.ads_sku_sales_top10
SELECT
    dt,
    sku_id,
    sku_name,
    category1_name,
    tm_name,
    sales_amount,
    sales_rank
FROM (
         SELECT
             dt,
             sku_id,
             sku_name,
             category1_name,
             tm_name,
             SUM(order_total_amount_1d) AS sales_amount,
             ROW_NUMBER() OVER (PARTITION BY dt ORDER BY SUM(order_total_amount_1d) DESC) AS sales_rank
         FROM bigdata_offline_v2_ws.dws_trade_user_sku_order_1d
         WHERE dt = cast(${bizdate} as string)
         GROUP BY dt, sku_id, sku_name, category1_name, tm_name
     ) t
WHERE sales_rank <= 10;


select * from bigdata_offline_v2_ws.ads_sku_sales_top10;

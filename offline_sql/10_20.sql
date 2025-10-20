03分层加不分层mysql


drop table if exists ads_product_360_summary_flat;
CREATE TABLE IF NOT EXISTS ads_product_360_summary_flat (
  product_id BIGINT COMMENT '商品ID',
  product_name VARCHAR(255) COMMENT '商品名称',
  category3_id BIGINT COMMENT '三级品类ID',
  category3_name VARCHAR(255) COMMENT '三级品类名称',
  sale_amount DECIMAL(16,2) COMMENT '销售额',
  sale_count BIGINT COMMENT '销售数量',
  uv_count BIGINT COMMENT '访客数',
  pv_count BIGINT COMMENT '浏览量',
  cart_count BIGINT COMMENT '加购数',
  favor_count BIGINT COMMENT '收藏数',
  conversion_rate DECIMAL(10,4) COMMENT '转化率',
  refund_amount DECIMAL(16,2) COMMENT '退款金额',
  refund_rate DECIMAL(10,4) COMMENT '退款率',
  avg_price DECIMAL(10,2) COMMENT '平均价格',
  stat_date VARCHAR(20) COMMENT '统计日期',
  ds VARCHAR(20) COMMENT '分区字段'
) COMMENT='商品360核心指标汇总表-不分层';

-- 插入数据（修正版MySQL语法）
INSERT INTO ads_product_360_summary_flat (
product_id, product_name, category3_id, category3_name,
sale_amount, sale_count, uv_count, pv_count, cart_count, favor_count,
conversion_rate, refund_amount, refund_rate, avg_price, stat_date, ds
)
  SELECT
  si.id as product_id,
  si.sku_name as product_name,
  si.category3_id,
  c3.name as category3_name,
  -- 销售额 - 使用正确的订单状态1002
  CAST(COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.split_total_amount ELSE 0 END), 0) AS DECIMAL(16,2)) as sale_amount,
  -- 销售数量
  COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.sku_num ELSE 0 END), 0) as sale_count,
  -- 访客数
  COALESCE(traffic.uv_count, 0) as uv_count,
  -- 浏览量
  COALESCE(traffic.pv_count, 0) as pv_count,
  -- 加购数
  COALESCE(cart.cart_count, 0) as cart_count,
  -- 收藏数
  COALESCE(favor.favor_count, 0) as favor_count,
  -- 转化率
  CAST(
  CASE
  WHEN COALESCE(traffic.uv_count, 0) > 0
  THEN COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.sku_num ELSE 0 END), 0) * 1.0 / COALESCE(traffic.uv_count, 1)
  ELSE 0
  END
  AS DECIMAL(10,4)) as conversion_rate,
  -- 退款金额
  CAST(COALESCE(refund.refund_amount, 0) AS DECIMAL(16,2)) as refund_amount,
  -- 退款率
  CAST(
  CASE
  WHEN COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.split_total_amount ELSE 0 END), 0) > 0
  THEN COALESCE(refund.refund_amount, 0) / COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.split_total_amount ELSE 0 END), 1)
  ELSE 0
  END
  AS DECIMAL(10,4)) as refund_rate,
  -- 平均价格
  CAST(
  CASE
  WHEN COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.sku_num ELSE 0 END), 0) > 0
  THEN COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.split_total_amount ELSE 0 END), 0) / COALESCE(SUM(CASE WHEN oi.order_status = '1002' THEN od.sku_num ELSE 0 END), 1)
  ELSE si.price
  END
  AS DECIMAL(10,2)) as avg_price,
  '20251017' as stat_date,
  '20251017' as ds
  FROM sku_info si
  LEFT JOIN base_category3 c3 ON si.category3_id = c3.id
  LEFT JOIN order_detail od ON si.id = od.sku_id
  LEFT JOIN order_info oi ON od.order_id = oi.id
  -- 流量数据子查询（修正GROUP BY问题）
  LEFT JOIN (
  SELECT
        product_id,
        COUNT(DISTINCT mid) as uv_count,
        COUNT(*) as pv_count
    FROM (
             SELECT
                 CAST(JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) AS UNSIGNED) as product_id,
                 JSON_UNQUOTE(JSON_EXTRACT(log, '$.common.mid')) as mid
             FROM z_log
                      CROSS JOIN JSON_TABLE(
                     JSON_EXTRACT(log, '$.displays'),
'$[*]' COLUMNS (display_item JSON PATH '$')
) AS displays
WHERE JSON_EXTRACT(log, '$.displays') IS NOT NULL
AND JSON_LENGTH(JSON_EXTRACT(log, '$.displays')) > 0
AND JSON_EXTRACT(display_item, '$.item') IS NOT NULL
AND JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item_type')) = 'sku_id'
AND JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) != ''
AND JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) REGEXP '^[0-9]+$'
) AS extracted_data
WHERE product_id IS NOT NULL
GROUP BY product_id
) traffic ON si.id = traffic.product_id
-- 加购数据子查询
LEFT JOIN (
    SELECT
        sku_id,
        COUNT(DISTINCT user_id) as cart_count
    FROM cart_info
    GROUP BY sku_id
) cart ON si.id = cart.sku_id
-- 收藏数据子查询
LEFT JOIN (
    SELECT
        sku_id,
        COUNT(DISTINCT user_id) as favor_count
    FROM favor_info
    GROUP BY sku_id
) favor ON si.id = favor.sku_id
-- 退款数据子查询
LEFT JOIN (
    SELECT
        sku_id,
        SUM(CASE WHEN refund_status = '0701' THEN refund_amount ELSE 0 END) as refund_amount
    FROM order_refund_info
    GROUP BY sku_id
) refund ON si.id = refund.sku_id
WHERE si.is_sale = 1
GROUP BY
si.id, si.sku_name, si.category3_id, c3.name, si.price,
traffic.uv_count, traffic.pv_count,
cart.cart_count, favor.favor_count,
refund.refund_amount;

-- DWD层：商品维度明细宽表（MySQL版本）
drop table if exists dwd_product_detail_wide;
CREATE TABLE IF NOT EXISTS dwd_product_detail_wide (
  product_id BIGINT COMMENT '商品ID',
  product_name VARCHAR(255) COMMENT '商品名称',
  category3_id BIGINT COMMENT '三级品类ID',
  category3_name VARCHAR(255) COMMENT '三级品类名称',
  spu_id BIGINT COMMENT 'SPU ID',
  price DECIMAL(10,2) COMMENT '价格',
  is_sale TINYINT COMMENT '是否在售',
  sale_amount DECIMAL(16,2) COMMENT '销售额',
  sale_count BIGINT COMMENT '销售数量',
  refund_amount DECIMAL(16,2) COMMENT '退款金额',
  cart_count BIGINT COMMENT '加购数',
  favor_count BIGINT COMMENT '收藏数',
  stat_date VARCHAR(20) COMMENT '统计日期',
  ds VARCHAR(20) COMMENT '分区字段'
) COMMENT='商品维度明细宽表';

-- 插入DWD层数据（MySQL版本）
INSERT INTO dwd_product_detail_wide (
product_id, product_name, category3_id, category3_name,
spu_id, price, is_sale, sale_amount, sale_count,
refund_amount, cart_count, favor_count, stat_date, ds
)
SELECT
    CAST(si.id AS UNSIGNED) as product_id,
CAST(si.sku_name AS CHAR) as product_name,
CAST(si.category3_id AS UNSIGNED) as category3_id,
CAST(c3.name AS CHAR) as category3_name,
CAST(si.spu_id AS UNSIGNED) as spu_id,
CAST(si.price AS DECIMAL(10,2)) as price,
CAST(si.is_sale AS UNSIGNED) as is_sale,
-- 销售额：使用独立的销售子查询
CAST(COALESCE(sales.sale_amount, 0) AS DECIMAL(16,2)) as sale_amount,
-- 销售数量
CAST(COALESCE(sales.sale_count, 0) AS UNSIGNED) as sale_count,
-- 退款金额：使用独立的退款子查询
CAST(COALESCE(refund.refund_amount, 0) AS DECIMAL(16,2)) as refund_amount,
-- 加购数：使用独立的加购子查询
CAST(COALESCE(cart.cart_count, 0) AS UNSIGNED) as cart_count,
-- 收藏数：使用独立的收藏子查询
CAST(COALESCE(favor.favor_count, 0) AS UNSIGNED) as favor_count,
CAST('20251017' AS CHAR) as stat_date,
'20251017' as ds
FROM sku_info si
LEFT JOIN base_category3 c3 ON si.category3_id = c3.id  -- 移除c3.ds条件

-- 独立的销售数据子查询
LEFT JOIN (
  SELECT
  od.sku_id,
  SUM(CASE WHEN oi.order_status = '1002' THEN od.split_total_amount ELSE 0 END) as sale_amount,
  SUM(CASE WHEN oi.order_status = '1002' THEN od.sku_num ELSE 0 END) as sale_count
  FROM order_detail od
  LEFT JOIN order_info oi ON od.order_id = oi.id  -- 移除oi.ds条件
  -- 移除od.ds条件，根据实际表结构调整
  GROUP BY od.sku_id
) sales ON si.id = sales.sku_id

-- 独立的退款数据子查询
LEFT JOIN (
  SELECT
  sku_id,
  SUM(CASE WHEN refund_status = '0701' THEN refund_amount ELSE 0 END) as refund_amount
  FROM order_refund_info
  -- 移除ds条件，根据实际表结构调整
  GROUP BY sku_id
) refund ON si.id = refund.sku_id

-- 独立的加购数据子查询
LEFT JOIN (
  SELECT
  sku_id,
  COUNT(DISTINCT user_id) as cart_count
  FROM cart_info
  -- 移除ds条件，根据实际表结构调整
  GROUP BY sku_id
) cart ON si.id = cart.sku_id

-- 独立的收藏数据子查询
LEFT JOIN (
  SELECT
  sku_id,
  COUNT(DISTINCT user_id) as favor_count
  FROM favor_info
  -- 移除ds条件，根据实际表结构调整
  GROUP BY sku_id
) favor ON si.id = favor.sku_id

WHERE si.is_sale = 1;  -- 移除si.ds条件



drop table if exists dws_product_traffic_agg;
CREATE TABLE IF NOT EXISTS dws_product_traffic_agg (
  product_id BIGINT COMMENT '商品ID',
  uv_count BIGINT COMMENT '访客数',
  pv_count BIGINT COMMENT '浏览量',
  avg_duration DECIMAL(10,2) COMMENT '平均停留时长',
  bounce_rate DECIMAL(10,4) COMMENT '跳出率',
  stat_date VARCHAR(20) COMMENT '统计日期',
  ds VARCHAR(20) COMMENT '分区字段'
) COMMENT='商品流量聚合表';

-- 插入DWS层数据（MySQL版本）
INSERT INTO dws_product_traffic_agg (
product_id, uv_count, pv_count, avg_duration, bounce_rate, stat_date, ds
)
SELECT
    base.product_id,
    COALESCE(traffic.uv_count, 0) as uv_count,
    -- PV计数应该是每个展示记录的计数，不是UV的去重计数
    COALESCE(traffic.pv_count, 0) as pv_count,
    CAST(0 AS DECIMAL(10,2)) as avg_duration,
    CAST(0 AS DECIMAL(10,4)) as bounce_rate,
CAST('20251017' AS CHAR) as stat_date,
CAST('20251017' AS CHAR) as ds
FROM (
         -- 基础商品数据
         SELECT CAST(id AS UNSIGNED) as product_id
         FROM sku_info
         WHERE is_sale = 1  -- 移除ds条件
     ) base
         LEFT JOIN (
    -- 流量统计数据（使用MySQL JSON函数）
    SELECT
        CAST(JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) AS UNSIGNED) as product_id,
        CAST(COUNT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(log, '$.common.mid'))) AS UNSIGNED) as uv_count,
        -- PV应该是所有展示记录的总数，不去重
        CAST(COUNT(*) AS UNSIGNED) as pv_count
    FROM z_log
             CROSS JOIN JSON_TABLE(
            JSON_EXTRACT(log, '$.displays'),
'$[*]' COLUMNS (display_item JSON PATH '$')
) AS displays
WHERE JSON_EXTRACT(log, '$.displays') IS NOT NULL
AND JSON_LENGTH(JSON_EXTRACT(log, '$.displays')) > 0
AND JSON_EXTRACT(display_item, '$.item') IS NOT NULL
AND JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item_type')) = 'sku_id'
AND JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) != ''
AND JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) REGEXP '^[0-9]+$'
GROUP BY CAST(JSON_UNQUOTE(JSON_EXTRACT(display_item, '$.item')) AS UNSIGNED)
) traffic ON base.product_id = traffic.product_id;

-- ADS层：商品360核心指标汇总表（MySQL版本）

drop table if exists ads_product_360_summary;
CREATE TABLE IF NOT EXISTS ads_product_360_summary (
  product_id BIGINT COMMENT '商品ID',
  product_name VARCHAR(255) COMMENT '商品名称',
  category3_id BIGINT COMMENT '三级品类ID',
  category3_name VARCHAR(255) COMMENT '三级品类名称',
  sale_amount DECIMAL(16,2) COMMENT '销售额',
  sale_count BIGINT COMMENT '销售数量',
  uv_count BIGINT COMMENT '访客数',
  pv_count BIGINT COMMENT '浏览量',
  cart_count BIGINT COMMENT '加购数',
  favor_count BIGINT COMMENT '收藏数',
  conversion_rate DECIMAL(10,4) COMMENT '转化率',
  refund_amount DECIMAL(16,2) COMMENT '退款金额',
  refund_rate DECIMAL(10,4) COMMENT '退款率',
  avg_price DECIMAL(10,2) COMMENT '平均价格',
  stat_date VARCHAR(20) COMMENT '统计日期',
  ds VARCHAR(20) COMMENT '分区字段'
) COMMENT='商品360核心指标汇总表';

-- 插入ADS层数据（MySQL版本）
INSERT INTO ads_product_360_summary (
product_id, product_name, category3_id, category3_name,
sale_amount, sale_count, uv_count, pv_count, cart_count, favor_count,
conversion_rate, refund_amount, refund_rate, avg_price, stat_date, ds
)
SELECT
    -- 第1-15列：数据列
    CAST(dwd.product_id AS UNSIGNED),
CAST(dwd.product_name AS CHAR),
CAST(dwd.category3_id AS UNSIGNED),
CAST(dwd.category3_name AS CHAR),
CAST(COALESCE(dwd.sale_amount, 0) AS DECIMAL(16,2)),
CAST(COALESCE(dwd.sale_count, 0) AS UNSIGNED),
CAST(COALESCE(dws.uv_count, 0) AS UNSIGNED),
CAST(COALESCE(dws.pv_count, 0) AS UNSIGNED),
CAST(COALESCE(dwd.cart_count, 0) AS UNSIGNED),
CAST(COALESCE(dwd.favor_count, 0) AS UNSIGNED),
CAST(
CASE
WHEN COALESCE(dws.uv_count, 0) > 0
THEN COALESCE(dwd.sale_count, 0) * 1.0 / COALESCE(dws.uv_count, 1)
ELSE 0
END
AS DECIMAL(10,4)),
CAST(COALESCE(dwd.refund_amount, 0) AS DECIMAL(16,2)),
CAST(
CASE
WHEN COALESCE(dwd.sale_amount, 0) > 0
THEN COALESCE(dwd.refund_amount, 0) / COALESCE(dwd.sale_amount, 1)
ELSE 0
END
AS DECIMAL(10,4)),
CAST(
CASE
WHEN COALESCE(dwd.sale_count, 0) > 0
THEN COALESCE(dwd.sale_amount, 0) / COALESCE(dwd.sale_count, 1)
ELSE 0
END
AS DECIMAL(10,2)),
CAST('20251017' AS CHAR),
-- 第16列：分区列
CAST('20251017' AS CHAR)
FROM dwd_product_detail_wide dwd
LEFT JOIN dws_product_traffic_agg dws ON dwd.product_id = dws.product_id AND dws.ds = '20251017'
WHERE dwd.ds = '20251017';

-- 修正后的测试SQL（MySQL版本）
SELECT
flat.product_id,
flat.product_name,
flat.sale_amount as 不分层_销售额,
sumry.sale_amount as 分层_销售额,
flat.uv_count as 不分层_访客数,
sumry.uv_count as 分层_访客数,
flat.conversion_rate as 不分层_转化率,
sumry.conversion_rate as 分层_转化率
FROM ads_product_360_summary_flat flat
JOIN ads_product_360_summary sumry ON flat.product_id = sumry.product_id
WHERE flat.ds = '20251017' AND sumry.ds = '20251017'
LIMIT 10;

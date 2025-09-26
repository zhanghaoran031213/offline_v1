use bigdata_offline_v2_ws;
--流量主题
--各渠道流量统计
-- create table bigdata_offline_v2_ws.ads_traffic_stats_by_channel_backup as select * from bigdata_offline_v2_ws.ads_traffic_stats_by_channel;
--
-- drop table bigdata_offline_v2_ws.ads_traffic_stats_by_channel;
--
-- CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_traffic_stats_by_channel
-- (
--     `dt`               STRING COMMENT '统计日期',
--     `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
--     `channel`          STRING COMMENT '渠道',
--     `uv_count`         BIGINT COMMENT '访客人数',
--     `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
--     `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
--     `sv_count`         BIGINT COMMENT '会话数',
--     `bounce_rate`      DECIMAL(38, 18) COMMENT '跳出率'
-- ) COMMENT '各渠道流量统计';
--
-- insert into bigdata_offline_v2_ws.ads_traffic_stats_by_channel select * from bigdata_offline_v2_ws.ads_traffic_stats_by_channel_backup;

--路径分析
create table bigdata_offline_v2_ws.ads_page_path_backup as select * from bigdata_offline_v2_ws.ads_page_path;

drop table bigdata_offline_v2_ws.ads_page_path;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_page_path
(
    `dt`         STRING COMMENT '统计日期',
    `source`     STRING COMMENT '跳转起始页面ID',
    `target`     STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析';

insert into bigdata_offline_v2_ws.ads_page_path select * from bigdata_offline_v2_ws.ads_page_path_backup;

--用户主题
--用户变动统计
create table bigdata_offline_v2_ws.ads_user_change_backup as select * from bigdata_offline_v2_ws.ads_user_change;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_user_change;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_user_change
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计';

insert into bigdata_offline_v2_ws.ads_user_change select * from bigdata_offline_v2_ws.ads_user_change_backup;

--用户留存率
create table bigdata_offline_v2_ws.ads_user_retention_backup as select * from bigdata_offline_v2_ws.ads_user_retention;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_user_retention;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_user_retention
(
    `dt`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(38, 18) COMMENT '留存率'
) COMMENT '用户留存率';

insert into bigdata_offline_v2_ws.ads_user_retention select * from bigdata_offline_v2_ws.ads_user_retention_backup;


--用户新增活跃统计
create table bigdata_offline_v2_ws.ads_user_stats_backup as select * from bigdata_offline_v2_ws.ads_user_stats;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_user_stats;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_user_stats
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计';

insert into bigdata_offline_v2_ws.ads_user_stats select * from bigdata_offline_v2_ws.ads_user_stats_backup;


--用户行为漏斗分析
create table bigdata_offline_v2_ws.ads_user_action_backup as select * from bigdata_offline_v2_ws.ads_user_action;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_user_action;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_user_action
(
    `dt`                STRING COMMENT '统计日期',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加购人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '用户行为漏斗分析';

insert into bigdata_offline_v2_ws.ads_user_action select * from bigdata_offline_v2_ws.ads_user_action_backup;


--新增下单用户统计
create table bigdata_offline_v2_ws.ads_new_order_user_stats_backup as select * from bigdata_offline_v2_ws.ads_new_order_user_stats;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_new_order_user_stats;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_new_order_user_stats
(
    `dt`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计';

insert into bigdata_offline_v2_ws.ads_new_order_user_stats select * from bigdata_offline_v2_ws.ads_new_order_user_stats_backup;


--最近 7 日内连续 3 日下单用户数
create table bigdata_offline_v2_ws.ads_order_continuously_user_count_backup as select * from bigdata_offline_v2_ws.ads_order_continuously_user_count;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_order_continuously_user_count;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_order_continuously_user_count
(
    `dt`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计';

insert into bigdata_offline_v2_ws.ads_order_continuously_user_count select * from bigdata_offline_v2_ws.ads_order_continuously_user_count_backup;


--商品主题
-- 最近 30 日各品牌复购率
create table bigdata_offline_v2_ws.ads_repeat_purchase_by_tm_backup as select * from bigdata_offline_v2_ws.ads_repeat_purchase_by_tm;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_repeat_purchase_by_tm
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '最近30日各品牌复购率统计';

insert into bigdata_offline_v2_ws.ads_repeat_purchase_by_tm select * from bigdata_offline_v2_ws.ads_repeat_purchase_by_tm_backup;



-- 各品牌商品下单统计
create table bigdata_offline_v2_ws.ads_order_stats_by_tm_backup as select * from bigdata_offline_v2_ws.ads_order_stats_by_tm;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_order_stats_by_tm;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_order_stats_by_tm
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`            STRING COMMENT '品牌ID',
    `tm_name`          STRING COMMENT '品牌名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品牌商品下单统计';

insert into bigdata_offline_v2_ws.ads_order_stats_by_tm select * from bigdata_offline_v2_ws.ads_order_stats_by_tm_backup;


--各品类商品下单统计
create table bigdata_offline_v2_ws.ads_order_stats_by_cate_backup as select * from bigdata_offline_v2_ws.ads_order_stats_by_cate;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_order_stats_by_cate;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_order_stats_by_cate
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`     STRING COMMENT '一级品类ID',
    `category1_name`   STRING COMMENT '一级品类名称',
    `category2_id`     STRING COMMENT '二级品类ID',
    `category2_name`   STRING COMMENT '二级品类名称',
    `category3_id`     STRING COMMENT '三级品类ID',
    `category3_name`   STRING COMMENT '三级品类名称',
    `order_count`      BIGINT COMMENT '下单数',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '各品类商品下单统计';

insert into bigdata_offline_v2_ws.ads_order_stats_by_cate select * from bigdata_offline_v2_ws.ads_order_stats_by_cate_backup;


--各品类商品购物车存量 Top3
create table bigdata_offline_v2_ws.ads_sku_cart_num_top3_by_cate_backup as select * from bigdata_offline_v2_ws.ads_sku_cart_num_top3_by_cate;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `sku_id`         STRING COMMENT 'SKU_ID',
    `sku_name`       STRING COMMENT 'SKU名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各品类商品购物车存量Top3';

insert into bigdata_offline_v2_ws.ads_sku_cart_num_top3_by_cate select * from bigdata_offline_v2_ws.ads_sku_cart_num_top3_by_cate_backup;


--各品牌商品收藏次数 Top3
create table bigdata_offline_v2_ws.ads_sku_favor_count_top3_by_tm_backup as select * from bigdata_offline_v2_ws.ads_sku_favor_count_top3_by_tm;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_sku_favor_count_top3_by_tm
(
    `dt`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3';

insert into bigdata_offline_v2_ws.ads_sku_favor_count_top3_by_tm select * from bigdata_offline_v2_ws.ads_sku_favor_count_top3_by_tm_backup;


-- 交易主题
--下单到支付时间间隔平均值
create table bigdata_offline_v2_ws.ads_order_to_pay_interval_avg_backup as select * from bigdata_offline_v2_ws.ads_order_to_pay_interval_avg;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_order_to_pay_interval_avg
(
    `dt`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计';

insert into bigdata_offline_v2_ws.ads_order_to_pay_interval_avg select * from bigdata_offline_v2_ws.ads_order_to_pay_interval_avg_backup;


--各省份交易统计
create table bigdata_offline_v2_ws.ads_order_by_province_backup as select * from bigdata_offline_v2_ws.ads_order_by_province;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_order_by_province;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_order_by_province
(
    `dt`                 STRING COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_code_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各省份交易统计';

insert into bigdata_offline_v2_ws.ads_order_by_province select * from bigdata_offline_v2_ws.ads_order_by_province_backup;


--优惠券主题
--优惠券使用统计
create table bigdata_offline_v2_ws.ads_coupon_stats_backup as select * from bigdata_offline_v2_ws.ads_coupon_stats;

DROP TABLE IF EXISTS bigdata_offline_v2_ws.ads_coupon_stats;
CREATE EXTERNAL TABLE bigdata_offline_v2_ws.ads_coupon_stats
(
    `dt`              STRING COMMENT '统计日期',
    `coupon_id`       STRING COMMENT '优惠券ID',
    `coupon_name`     STRING COMMENT '优惠券名称',
    `used_count`      BIGINT COMMENT '使用次数',
    `used_user_count` BIGINT COMMENT '使用人数'
) COMMENT '优惠券使用统计';

insert into bigdata_offline_v2_ws.ads_coupon_stats select * from bigdata_offline_v2_ws.ads_coupon_stats_backup;









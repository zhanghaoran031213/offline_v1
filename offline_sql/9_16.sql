drop table if exists ods_activity_rule;
create external table bigdata_offline_v1_ws.ods_activity_rule(
     id               int    comment '编号',
     activity_id      int    comment '类型',
     activity_type    STRING comment '活动类型',
     condition_amount STRING comment '满减金额',
     condition_num    int    comment '满减件数',
     benefit_amount   STRING comment '优惠金额',
     benefit_discount STRING comment '优惠折扣',
     benefit_level    int    comment '优惠级别'
)
    PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/ods_activity_rule/';

drop table ods_activity_rule;
show databases ;
use bigdata_offline_v1_ws;
show tables ;
select * from ods_activity_rule where ds = '20250915';
CREATE DATABASE IF NOT EXISTS jtp_realtime_db;
USE jtp_realtime_db;
DROP TABLE IF EXISTS jtp_realtime_db.dws_traffic_page_view_window_report;
CREATE TABLE jtp_realtime_db.dws_traffic_page_view_window_report
(
    `windowStartTime` DATETIME COMMENT '窗口开始时间',
    `windowEndTime`   DATETIME COMMENT '窗口结束时间',
    -- 分区字段
    cur_date          DATE COMMENT '当天日期',
    `brand`           VARCHAR(255) COMMENT '品牌',
    `channel`         VARCHAR(255) COMMENT '渠道',
    `province`        VARCHAR(255) COMMENT '省份',
    `isNew`           TINYINT(4) COMMENT '是否新用户',
    `uvCount`         BIGINT REPLACE COMMENT 'UV数',
    `sessionCount`    BIGINT REPLACE COMMENT '会话数',
    `pvCount`         BIGINT REPLACE COMMENT 'PV数',
    `pvDuringTime`    BIGINT REPLACE COMMENT 'PV停留时长'

) ENGINE =OLAP
    AGGREGATE KEY(`windowStartTime`, `windowEndTime`, cur_date, `brand`, `channel`, `province`, `isNew`)
    COMMENT '交易订单sku汇总表'
    PARTITION BY RANGE(`windowStartTime`) (
        PARTITION jtp_realtime_db_table_2025 VALUES [('2025-08-14'), ('2025-08-15'))
    )
DISTRIBUTED BY HASH(`windowStartTime`) BUCKETS 3
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "par",
    "dynamic_partition.buckets" = "3",
    "replication_num" = "1"
);

SHOW PARTITIONS FROM jtp_realtime_db.dws_traffic_page_view_window_report;

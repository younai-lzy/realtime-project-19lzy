-- 创建表
DROP TABLE IF EXISTS jtp_realtime_db.dws_traffic_search_keyword_window_report ;
CREATE TABLE IF NOT EXISTS jtp_realtime_db.dws_traffic_search_keyword_window_report
(
    -- 时间窗口，开始时间和结束时间
    `window_start_time` DATETIME COMMENT '计算窗口开始日期时间',
    `window_end_time`   DATETIME COMMENT '计算窗口结束日期时间',
    -- 分区字段
    `cur_date`          DATE COMMENT '当天日期',
    -- 统计维度（粒度）
    `keyword`           VARCHAR(256) COMMENT '搜索关键词',
    -- 统计指标
    `keyword_count`     BIGINT REPLACE COMMENT '搜索关键词被搜索次数'
) ENGINE = OLAP AGGREGATE KEY (`window_start_time`,`window_end_time`,`cur_date`,`keyword`)
    COMMENT "搜索关键词窗口汇总表"
    PARTITION BY RANGE(`cur_date`)(
        PARTITION par20240418 VALUES [("2024-04-18"), ("2024-04-19")),
        PARTITION par20250814 VALUES [("2025-08-14"), ("2025-08-15")),
        PARTITION par20250815 VALUES [("2025-08-15"), ("2025-08-16")),
        PARTITION par20250816 VALUES [("2025-08-16"), ("2025-08-17")),
        PARTITION par20250817 VALUES [("2025-08-17"), ("2025-08-18")),
        PARTITION par20250818 VALUES [("2025-08-18"), ("2025-08-19"))
    )
    DISTRIBUTED BY HASH(`window_start_time`) BUCKETS 3
    properties (
        "replication_num" = "1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "par",
        "dynamic_partition.buckets" = "3"
    );


-- 查看是否创建成功
SHOW PARTITIONS FROM jtp_realtime_db.dws_traffic_search_keyword_window_report;

-- 查询数据
SELECT * FROM jtp_realtime_db.dws_traffic_search_keyword_window_report ;
USE jtp_realtime_db;
CREATE TABLE IF NOT EXISTS dwd_traffic_page_log_kafka_source
(
    `common` MAP<STRING, STRING> COMMENT '公共环境信息',
    `page`   MAP<STRING, STRING> COMMENT '页面信息',
    `ts`     BIGINT,
    row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss.SSS')),
    WATERMARK FOR row_time AS row_time - INTERVAL '0' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd-traffic-page-log',
    'properties.bootstrap.servers' = 'node101:9092,node102:9092,node103:9092',
    'properties.group.id' = 'gid_dws_traffic_search_keyword',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
;

CREATE TABLE IF NOT EXISTS dwd_traffic_search_keyword_window_report_doris_sink
(
    `window_start_time` DATETIME COMMENT '计算窗口开始日期时间',
    window_end_time   DATETIME COMMENT '计算窗口结束日期时间',
    `cur_date`          DATE COMMENT '当天日期',
    `keyword`           VARCHAR(256) COMMENT '搜索关键词',
    `keyword_count`     BIGINT REPLACE COMMENT '搜索关键词被搜索次数'
) WITH (
    'connector' = 'doris',
    'fenodes.enable' = 'node102:8030',
    'table.identifier' = 'jtp_realtime_db.dws_traffic_search_keyword_window_report',
    'username' = 'root',
    'password' = '123456',
    'sink.label-prefix' = 'doris_label'
)

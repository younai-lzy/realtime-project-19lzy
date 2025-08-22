-- 创建flinksql表
CREATE TABLE dwd_mapping_to_kafka_source
(
    `tradingDate`   STRING,
    `code`          BIGINT,
    `stockCode`     STRING,
    `stockName`     STRING,
    `volume`        BIGINT,
    `changePercent` DOUBLE,
    `openPrice`     DOUBLE,
    `highPrice`     DOUBLE,
    `maxPrice`      DOUBLE,
    `minPrice`      DOUBLE,
    `latestPrice`   DOUBLE,
    `prevClose`     DOUBLE,
    `typeFlag`      STRING,
    `updateTime`    STRING,
    `row_time` AS TO_TIMESTAMP(CONCAT(`tradingDate`, ' ', `updateTime`), 'yyyy-MM-dd HH:mm:ss.SSS'),
    WATERMARK FOR `row_time` AS `row_time` - INTERVAL '3' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_market_tick',
  'properties.bootstrap.servers' = 'node101:9092,node102:9092,node103:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

-- CREATE TABLE dws_security_day_open_price_report_sink
-- (
--     id   INT,
--     name STRING,
--     age  INT
-- )
-- WITH ( 'connector' = 'doris',
--     'fenodes' = 'node102:8030',
--     'table.identifier' = 'test.student',
--     'username' = 'root',
--     'password' = 'password',
--     'sink.label-prefix' = 'doris_label'
--     --'sink.enable.batch-mode' = 'true'  增加该配置可以走攒批写入
-- );
SELECT
    -- 获取窗口的开始时间作为交易日
    TUMBLE_START(`row_time`, INTERVAL '1' DAY) AS `tradingDate`,
    -- 股票代码
    `stockCode`,
    -- 今开价：窗口内的第一笔成交价格
    FIRST_VALUE(`openPrice`)                   AS `openPrice`,
    -- 收盘价：窗口内的最后一笔成交价格
    LAST_VALUE(`latestPrice`)                  AS `closePrice`,
    -- 最高价：窗口内的最高价格
    MAX(`highPrice`)                           AS `highPrice`,
    -- 最低价：窗口内的最低价格
    MIN(`minPrice`)                            AS `lowPrice`,
    -- 总成交量：窗口内的所有成交量总和
    SUM(`volume`)                              AS `totalVolume`,
    -- 总成交额：窗口内的所有成交额总和
    SUM(`changePercent`)                       AS `totalAmount`
FROM dwd_mapping_to_kafka_source
-- 定义日级滚动窗口，并按股票代码分组
GROUP BY `stockCode`,
         TUMBLE(`row_time`, INTERVAL '1' DAY)


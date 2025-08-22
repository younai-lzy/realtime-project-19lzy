CREATE DATABASE IF NOT EXISTS jtp_security_db;
USE jtp_security_db;

-- 修改后的
CREATE TABLE IF NOT EXISTS jtp_security_db.dws_market_stock_daily_report_sink
(
    -- 1. 定义 KEY 列，用于唯一标识一行数据
    `tradingDate` DATEV2 NOT NULL COMMENT '交易日期',
    `stockCode`   VARCHAR(20) NOT NULL COMMENT '股票代码',

    -- 2. 定义 VALUE 列，并指定聚合类型
    -- 开盘价：FIRST_VALUE 聚合函数对应 Doris 的 REPLACE
    `openPrice` DECIMALV3(10, 3) REPLACE COMMENT '开盘价',

    -- 收盘价：LAST_VALUE 聚合函数对应 Doris 的 REPLACE
    `closePrice` DECIMALV3(10, 3) REPLACE COMMENT '收盘价',

    -- 最高价：MAX 聚合函数对应 Doris 的 MAX
    `highPrice` DECIMALV3(10, 3) MAX COMMENT '最高价',

    -- 最低价：MIN 聚合函数对应 Doris 的 MIN
    `lowPrice` DECIMALV3(10, 3) MIN COMMENT '最低价',

    -- 总成交量：SUM 聚合函数对应 Doris 的 SUM
    `totalVolume` BIGINT SUM COMMENT '总成交量',

    -- 总成交额：SUM 聚合函数对应 Doris 的 SUM
    `totalAmount` DECIMALV3(20, 2) SUM COMMENT '总成交额',

    -- 涨跌幅：在 Doris 的 Agg 模型中，直接将计算结果作为 REPLACE 聚合
    `changePercent` DECIMALV3(10, 4) REPLACE COMMENT '涨跌幅'
)
-- 指定使用 Aggregate 模型
    AGGREGATE KEY(`tradingDate`, `stockCode`)
-- 按天进行分区，提高查询性能
PARTITION BY RANGE(`tradingDate`) (
    PARTITION p20250820 VALUES [('2025-08-20'), ('2025-08-21')),
    PARTITION p20250821 VALUES [('2025-08-21'), ('2025-08-22'))
)
-- 按股票代码进行分桶，实现数据均匀分布和并行查询
DISTRIBUTED BY HASH(`stockCode`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-8",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);

SELECT `tradingDate`,
       `stockCode`,
       `openPrice`,
       `closePrice`,
       `highPrice`,
       `lowPrice`,
       `totalVolume`,
       `totalAmount`,
       -- 使用计算出的收盘价和开盘价来计算涨跌幅
       ((`closePrice` - `openPrice`) / `openPrice`) * 100 AS `changePercent`
FROM (SELECT
          -- 获取窗口的开始时间作为交易日
          TUMBLE_START(`row_time`, INTERVAL '1' DAY) AS `tradingDate`,
          -- 股票代码
          `stockCode` AS `stockCode`,
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
          SUM(`amount`)                              AS `totalAmount`
      FROM dwd_mapping_to_kafka_source

           -- 定义日级滚动窗口，并按股票代码分组
      GROUP BY `stockCode`,
               TUMBLE(`row_time`, INTERVAL '1' DAY));
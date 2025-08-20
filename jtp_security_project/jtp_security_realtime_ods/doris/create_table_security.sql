CREATE DATABASE IF NOT EXISTS jtp_security_db;
USE jtp_security_db;
DROP TABLE IF EXISTS jtp_security_db.ods_stock_snapshot;
CREATE TABLE jtp_security_db.ods_stock_snapshot
(
    market_id  VARCHAR(20) COMMENT '行情源ID，例如 MD001',
    symbol     VARCHAR(20) COMMENT '证券代码，例如 000001',
    name       VARCHAR(50) COMMENT '证券名称，例如 上证指数',
    volume     BIGINT COMMENT '成交量（股/手）',
    amount     DECIMAL(20, 2) COMMENT '成交额（元/万元/美元等，按实际行情单位）',
    open       DECIMAL(16, 2) COMMENT '今开盘价',
    pre_close  DECIMAL(16, 2) COMMENT '昨日收盘价',
    high       DECIMAL(16, 2) COMMENT '当日最高价',
    low        DECIMAL(16, 2) COMMENT '当日最低价',
    price      DECIMAL(16, 2) COMMENT '最新成交价',
    close      DECIMAL(16, 2) COMMENT '收盘价（实时快照可能等于最新价）',
    trade_day  DATE COMMENT '交易日期，例如 2025-08-20',
    trade_time VARCHAR(50) COMMENT '交易时间戳，例如 11:19:36.550'
)
    DUPLICATE KEY(market_id, symbol)
PARTITION BY RANGE (trade_day) ()
DISTRIBUTED BY HASH(symbol) BUCKETS 3
PROPERTIES(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",   -- 保留过去 3 天
    "dynamic_partition.end" = "3",      -- 提前创建未来 3 天
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "3",
    "replication_num" = "1"
);


package com.sina.security.jtp.dws.job;

import com.sina.security.jtp.common.utils.KafkaUtil;
import com.sina.security.jtp.dws.function.SecurityProcessDayOpenFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author 30777
 */
public class JtpSecurityDayOpenPriceJob {
  public static void main(String[] args) throws Exception {
    //创建表环境
    TableEnvironment tEnv = getTableEnv();
    //创建输入表
    createInputTable(tEnv);
    //查询指标今开
    Table reportTable = queryDayOpenPrice(tEnv);

    //4.输出表-Output：映射到Doris表
    //createOutputTable(tEnv);

    //5.保存数据
    //saveToDoris(tEnv, reportTable);

  }

  /**
   * 输出表-Output：映射到Doris表
   * @param tEnv
   */
  private static void createOutputTable(TableEnvironment tEnv) {
    tEnv.executeSql("CREATE TABLE IF NOT EXISTS dws_security_day_open_price_report_sink\n" +
      "(\n" +
      "    `tradeDate`   STRING,\n" +
      "    `code`        BIGINT,\n" +
      "    `openPrice`   DECIMAL,\n" +
      "    `ts`          BIGINT\n" +
      ") WITH (\n" +
      "  'connector' = 'doris',\n" +
      ""
    )

  }

  /**
   * 查询指标今开
   * @param tEnv
   */
  private static Table queryDayOpenPrice(TableEnvironment tEnv) {
    Table reportTable = tEnv.sqlQuery(
      "SELECT `tradingDate`,\n" +
        "       `stockCode`,\n" +
        "       `openPrice`,\n" +
        "       `closePrice`,\n" +
        "       `highPrice`,\n" +
        "       `lowPrice`,\n" +
        "       `totalVolume`,\n" +
        "       `totalAmount`,\n" +
        "       ((`closePrice` - `openPrice`) / `openPrice`) * 100 AS `changePercent`\n" +
        "FROM (SELECT\n" +
        "          TUMBLE_START(`row_time`, INTERVAL '1' DAY) AS `tradingDate`,\n" +
        "          `stockCode` AS `stockCode`,\n" +
        "          FIRST_VALUE(`openPrice`)                   AS `openPrice`,\n" +
        "          LAST_VALUE(`latestPrice`)                  AS `closePrice`,\n" +
        "          MAX(`highPrice`)                           AS `highPrice`,\n" +
        "          MIN(`minPrice`)                            AS `lowPrice`,\n" +
        "          SUM(`volume`)                              AS `totalVolume`,\n" +
        "          SUM(`amount`)                              AS `totalAmount`\n" +
        "      FROM dwd_mapping_to_kafka_source\n" +
        "\n" +
        "           -- 定义日级滚动窗口，并按股票代码分组\n" +
        "      GROUP BY `stockCode`,\n" +
        "               TUMBLE(`row_time`, INTERVAL '1' DAY))"
    );

    //返回计算结果
    return reportTable;
  }

  /**
   * FlinkSQL中输入表，构建DDL语句，创建FlinkSQL表映射到kafka消息队列
   */
  private static void createInputTable(TableEnvironment tableEnv) {
    tableEnv.executeSql(
      "CREATE TABLE dwd_mapping_to_kafka_source\n" +
        "(\n" +
        "    `tradingDate`   STRING,\n" +
        "    `code`          BIGINT,\n" +
        "    `stockCode`     STRING,\n" +
        "    `stockName`     STRING,\n" +
        "    `volume`        BIGINT,\n" +
        "    `changePercent` DOUBLE,\n" +
        "    `openPrice`     DOUBLE,\n" +
        "    `highPrice`     DOUBLE,\n" +
        "    `maxPrice`      DOUBLE,\n" +
        "    `minPrice`      DOUBLE,\n" +
        "    `latestPrice`   DOUBLE,\n" +
        "    `prevClose`     DOUBLE,\n" +
        "    `typeFlag`      STRING,\n" +
        "    `updateTime`    STRING,\n" +
        "    `row_time` AS TO_TIMESTAMP(CONCAT(`tradingDate`, ' ', `updateTime`), 'yyyy-MM-dd HH:mm:ss.SSS'),\n" +
        "    WATERMARK FOR `row_time` AS `row_time` - INTERVAL '0' SECOND\n" +
        ") WITH (\n" +
        "  'connector' = 'kafka',\n" +
        "  'topic' = 'dwd_market_tick',\n" +
        "  'properties.bootstrap.servers' = 'node101:9092,node102:9092,node103:9092',\n" +
        "  'properties.group.id' = 'testGroup',\n" +
        "  'scan.startup.mode' = 'earliest-offset',\n" +
        "  'format' = 'json',\n" +
        "  'json.fail-on-missing-field' = 'false',\n" +
        "  'json.ignore-parse-errors' = 'true'\n" +
        ")"
    );
  }


  /**
   * 获取表环境
   */
  private static TableEnvironment getTableEnv() {
    //1.环境设置
    EnvironmentSettings settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build();

    TableEnvironment tEnv = TableEnvironment.create(settings);
    //2.配置属性设置
    Configuration configuration = tEnv.getConfig().getConfiguration();
    //本地时区
    configuration.setString("table.local-time-zone", "Asia/Shanghai");
    //默认并行度
    configuration.setString("table.exec.resource.default-parallelism", "1");
    // 设置最大并发检查点数量。1 意味着在任何时候，都只允许一个检查点正在进行。
    // 这可以避免同时启动多个检查点任务，从而减少系统负载和网络压力
    configuration.setString("execution.checkpointing.max-concurrent-checkpoints", "1");
    configuration.setString("state.checkpoint-interval", "5000");
    //表示每5秒触发一次检查点
    configuration.setString("execution.checkpointing.interval", "5 s");
    //设置检查点的语义。EXACTLY_ONCE 保证了在故障恢复后，每条数据都会且仅会被处理一次，
    // 这是最强的容错保证，也是默认和推荐的设置
    configuration.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
    // 设置状态存储
    //rocksdb 是一个嵌入式的键值存储，它将状态数据存储在本地磁盘上，
    // 因此它可以处理比内存更多的状态，并且支持增量检查点，非常适合大状态的作业
    configuration.setString("state.backend", "rocksdb");
    configuration.setBoolean("state.backend.incremental", true);
    // 优化：State TTL 时间
    //它设置了状态数据在没有被访问后可以存活的最长时间。当一个状态超过这个时间后，会被自动清除。
    //这有助于防止状态的无限增长，节省存储空间，并提高性能。
    configuration.setString("table.exec.state.ttl", "5 s");
    return tEnv;

  }
}

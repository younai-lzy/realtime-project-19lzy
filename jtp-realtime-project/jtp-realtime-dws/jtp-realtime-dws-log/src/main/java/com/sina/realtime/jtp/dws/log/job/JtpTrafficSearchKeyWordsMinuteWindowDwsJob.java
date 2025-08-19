package com.sina.realtime.jtp.dws.log.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 搜索关键词实时统计，其中使用IkAnalyzer进行分词,采用Flink SQL实现数据处理
 * @author 30777
 */
public class JtpTrafficSearchKeyWordsMinuteWindowDwsJob {
  public static void main(String[] args) {
    //1.创建环境
    TableEnvironment tableEnv = getTableEnv();
    //2.输入表-Input：映射到Kafka消息对列
    createInputTable(tableEnv);
    tableEnv.executeSql("SELECT * FROM dwd_traffic_page_log_kafka_source").print();


    //3.处理表-Process：进行分词处理
    // Table reportTable = createProcessTable(tableEnv);

    //4.输出表-Output：映射到Doris表
    //createOutputTable(tableEnv);

    //5.保存数据
    //saveToDoris(tableEnv, reportTable);
  }

  /**
   * FlinkSQL中将查询结果保存到Doris表中,使用INSERT语句
   */
  private static void saveToDoris(TableEnvironment tableEnv, Table reportTable) {
  }

  /**
   * FlinkSQL中输出表，构建DDL语句，创建FlinkSQL表映射到Doris表
   */
  private static void createOutputTable(TableEnvironment tableEnv) {
    tableEnv.executeSql(
      "CREATE TABLE IF NOT EXISTS dwd_traffic_search_keyword_window_report_doris_sink\n" +
        "(\n" +
        "    `window_start_time` DATETIME COMMENT '计算窗口开始日期时间',\n" +
        "    window_end_time   DATETIME COMMENT '计算窗口结束日期时间',\n" +
        "    `cur_date`          DATE COMMENT '当天日期',\n" +
        "    `keyword`           VARCHAR(256) COMMENT '搜索关键词',\n" +
        "    `keyword_count`     BIGINT REPLACE COMMENT '搜索关键词被搜索次数'\n" +
        ") WITH (\n" +
        "    'connector' = 'doris',\n" +
        "    'fenodes.enable' = 'node102:8030',\n" +
        "    'table.identifier' = 'jtp_realtime_db.dws_traffic_search_keyword_window_report',\n" +
        "    'username' = 'root',\n" +
        "    'password' = '123456',\n" +
        "    'sink.label-prefix' = 'doris_label'\n" +
        ")"
    );
  }

  /**
   * FlinkSQL中编写SELECT查询语句，处理分析数据
   */
  private static Table createProcessTable(TableEnvironment tableEnv) {
    return null;
  }

  /**
   * FlinkSQL中输入表，构建DDL语句，创建FLinkSQL表映射到kafka消息队列
   */
  private static void createInputTable(TableEnvironment tableEnv) {
    tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_traffic_page_log_kafka_source\n" +
      "(\n" +
      "    `common` MAP<STRING, STRING> COMMENT '公共环境信息',\n" +
      "    `page`   MAP<STRING, STRING> COMMENT '页面信息',\n" +
      "    `ts`     BIGINT,\n" +
      "    row_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss.SSS')),\n" +
      "    WATERMARK FOR row_time AS row_time - INTERVAL '0' MINUTE\n" +
      ") WITH (\n" +
      "    'connector' = 'kafka',\n" +
      "    'topic' = 'dwd-traffic-page-log',\n" +
      "    'properties.bootstrap.servers' = 'node101:9092,node102:9092,node103:9092',\n" +
      "    'properties.group.id' = 'gid_dws_traffic_search_keyword',\n" +
      "    'scan.startup.mode' = 'earliest-offset',\n" +
      "    'format' = 'json',\n" +
      "    'json.fail-on-missing-field' = 'false',\n" +
      "    'json.ignore-parse-errors' = 'true'\n" +
      ")");

  }

  /**
   * 构建FlinkSQL表执行环境，用于读取数据、处理数据、执行SQL语句等操作
   * @return
   */
  private static TableEnvironment getTableEnv() {
    //1.环境设置
    EnvironmentSettings settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build();

    TableEnvironment tabEnv = TableEnvironment.create(settings);
    //2.配置属性设置
    Configuration configuration = tabEnv.getConfig().getConfiguration();
    configuration.setString("table.local-time-zone", "Asia/Shanghai");
    configuration.setString("table.exec.resource.default-parallelism", "1");
    configuration.setString("table.exec.state.ttl", "5 s");
    configuration.setString("execution.checkpointing.interval", "5 s");

    //3-返回对象
    return tabEnv;
  }


}

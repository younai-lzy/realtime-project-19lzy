package com.sina.realtime.jtp.dws.log.job;

import com.sina.realtime.jtp.dws.log.function.IkAnalyzerFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

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
    //查询结果
    //tableEnv.executeSql("SELECT * FROM dwd_traffic_page_log_kafka_source").print();


    //3.处理表-Process：进行分词处理
    Table reportTable = createProcessTable(tableEnv);

    //4.输出表-Output：映射到Doris表
    createOutputTable(tableEnv);

    //5.保存数据
    saveToDoris(tableEnv, reportTable);
  }

  /**
   * FlinkSQL中将查询结果保存到Doris表中,使用INSERT语句
   */
  private static void saveToDoris(TableEnvironment tableEnv, Table reportTable) {
    //1.注册结果数据为表
    tableEnv.createTemporaryView("report_table", reportTable);
    //2.执行查询插入语句
    tableEnv.executeSql(
      "INSERT INTO dws_traffic_search_keyword_window_report_doris_sink\n" +
        "SELECT\n" +
        "    substring(CAST(window_start_time AS STRING), 0, 19) AS start_time,\n" +
        "    substring(CAST(window_end_time AS STRING), 0, 19) AS end_time,\n" +
        "    substring(CAST(window_start_time AS STRING), 0, 10) AS cur_date,\n" +
        "    keyword,\n" +
        "    keyword_count\n" +
        "FROM report_table"
    );
  }

  /**
   * FlinkSQL中输出表，构建DDL语句，创建FlinkSQL表映射到Doris表
   * todo 相当于建了一个临时表与doris表进行映射
   */
  private static void createOutputTable(TableEnvironment tableEnv) {
    tableEnv.executeSql(
      "CREATE TABLE dws_traffic_search_keyword_window_report_doris_sink\n" +
        "(\n" +
        "    `window_start_time` STRING COMMENT '计算窗口开始日期时间',\n" +
        "    window_end_time     STRING COMMENT '计算窗口结束日期时间',\n" +
        "    `cur_date`          STRING COMMENT '当天日期',\n" +
        "    `keyword`           STRING COMMENT '搜索关键词',\n" +
        "    `keyword_count`     BIGINT COMMENT '搜索关键词被搜索次数'\n" +
        ") WITH (\n" +
        "    'connector' = 'doris',\n" +
        "    'fenodes' = 'node102:8030',\n" +
        "    'table.identifier' = 'jtp_realtime_db.dws_traffic_search_keyword_window_report',\n" +
        "    'username' = 'root',\n" +
        "    'password' = '123456',\n" +
        "    'sink.label-prefix' = 'doris_label'\n" +
        ")"
    );
  }


  /**
   * 使用FlinkSQL进行分词处理
   */
  private static Table createProcessTable(TableEnvironment tableEnv) {
    //s1.获取搜索词和搜索时间
    Table searchLogTable = tableEnv.sqlQuery(
      "SELECT\n" +
        "    `page`['item'] AS full_word\n" +
        "    , row_time\n" +
        "FROM dwd_traffic_page_log_kafka_source\n" +
        "WHERE `page`['item'] IS NOT NULL\n" +
        "AND `page`['last_page_id'] = 'search'\n" +
        "AND `page`['item_type'] = 'keyword'"
    );
    //searchLogTable.execute().print();
    tableEnv.createTemporaryView("search_log_table2", searchLogTable);

    //s2.使用自定义UDTF函数，对搜索词进行中文分词
    tableEnv.createTemporarySystemFunction("ik_analyzer_udtf", IkAnalyzerFunction.class);

    Table wordLogTable = tableEnv.sqlQuery(
      "SELECT full_word, keyword, row_time\n" +
      "FROM search_log_table2,\n" +
      "LATERAL TABLE(ik_analyzer_udtf(full_word)) AS T(keyword)");
    //wordLogTable.execute().print();

    tableEnv.createTemporaryView("word_log_table2", wordLogTable);

    //s3.设置窗口进行分组、聚合计算
    Table reportTable = tableEnv.sqlQuery(
      "SELECT\n" +
        "    window_start AS window_start_time,\n" +
        "    window_end AS window_end_time,\n" +
        "    keyword,\n" +
        "    COUNT(keyword) AS keyword_count,\n" +
        "    UNIX_TIMESTAMP() * 1000 AS ts\n" +
        "    FROM TABLE(\n" +
        "      TUMBLE(TABLE word_log_table2, DESCRIPTOR(row_time), INTERVAL '1' MINUTES)\n" +
        "    )t1\n" +
        "    GROUP BY window_start, window_end, keyword"
    );
    //reportTable.execute().print();
    //返回计算结果
    return reportTable;

  }

  /**
   * FlinkSQL中输入表，构建DDL语句，创建FLinkSQL表映射到kafka消息队列
   */
  private static void createInputTable(TableEnvironment tableEnv) {
    tableEnv.executeSql(
      "CREATE TABLE dwd_traffic_page_log_kafka_source\n" +
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

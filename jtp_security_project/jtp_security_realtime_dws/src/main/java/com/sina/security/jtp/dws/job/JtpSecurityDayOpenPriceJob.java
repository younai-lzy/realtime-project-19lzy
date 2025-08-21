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


  }

  /**
   * 处理数据
   */
//  private static DataStream<Tuple3<String, String, Double>> processData(DataStream<String> stream) {
//    //s1.将数据转为实体类对象
//    SingleOutputStreamOperator<StockData> map = stream.map(new MapFunction<String, StockData>() {
//      @Override
//      public StockData map(String s) throws Exception {
//        String[] split = s.split("\\|");
//        return new StockData(split[0], split[1], split[2], split[3],
//          Long.parseLong(split[4]),
//          new BigDecimal(split[5]), Double.parseDouble(split[6]),
//          Double.parseDouble(split[7]), Double.parseDouble(split[8]),
//          Double.parseDouble(split[9]), Double.parseDouble(split[10]),
//          Double.parseDouble(split[11]),
//          split[12],
//          split[13]
//        );
//
//      }
//    });
//    //s2.设置水位线
//    SingleOutputStreamOperator<StockData> waterMarks = map.assignTimestampsAndWatermarks(WatermarkStrategy.<StockData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
//      .withTimestampAssigner(new SerializableTimestampAssigner<StockData>() {
//
//        @Override
//        public long extractTimestamp(StockData stockData, long l) {
//          //获取交易日期
//          String tradingDate = stockData.getTradingDate();
//          String updateTime = stockData.getUpdateTime();
//          String ts = tradingDate + " " + updateTime;
//          //将时间转化为毫秒
//          // 2. 定义与字符串格式匹配的 DateTimeFormatter
//          // 注意：.SSS 表示毫秒，不区分大小写
//          DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//
//          // 3. 将字符串解析为 LocalDateTime 对象
//          LocalDateTime localDateTime = LocalDateTime.parse(ts, formatter);
//
//          // 4. 将 LocalDateTime 转换为毫秒时间戳
//          // 需要指定时区偏移量，这里使用东八区（+8）
//          long timestampMillis = localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
//
//          //System.out.println("原始字符串: " + ts);
//          //System.out.println("转换后的毫秒时间戳: " + timestampMillis);
//          return timestampMillis;
//        }
//      })
//    );
//
//    DataStream<Tuple3<String, String, Double>> openPrices = waterMarks.keyBy(stockData -> stockData.getCode())
//      //划分秒级滚动窗口
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .process(new SecurityProcessDayOpenFunction());
//
//    //s5.输出
//    //openPrices.print();
//    return openPrices;
//
//  }
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

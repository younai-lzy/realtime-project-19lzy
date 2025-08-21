package com.sina.security.jtp.dwd.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sina.security.jtp.common.utils.KafkaUtil;
import com.sina.security.jtp.dwd.pojo.StockData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.math.BigDecimal;

/**
 * @author 30777
 */
public class JtpSecurityOdsToDwd {
  public static void main(String[] args) throws Exception {
    //创建环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //读取数据
    DataStream<String> marketData = KafkaUtil.consumerKafka(env, "market_data");


    // 解析成 MarketTick 对象
    DataStream<String> dwdStream = marketData.map(line -> {
      try {
        String[] arr = line.split("\\|");
        StockData tick = new StockData();
        tick.tradingDate = arr[0]; // 补交易日期
        tick.code = arr[1];
        tick.stockCode = arr[2];
        tick.stockName = arr[3];
        tick.volume = Long.parseLong(arr[4]);
        tick.changePercent = new BigDecimal(arr[5]);
        tick.openPrice = Double.parseDouble(arr[6]);
        tick.highPrice = Double.parseDouble(arr[7]);
        tick.maxPrice = Double.parseDouble(arr[8]);
        tick.minPrice = Double.parseDouble(arr[9]);
        tick.latestPrice = Double.parseDouble(arr[10]);
        tick.prevClose = Double.parseDouble(arr[11]);
        tick.typeFlag = arr[12];
        tick.updateTime = arr[13];
        /**
         * new ObjectMapper()
         *
         * ObjectMapper 是 Jackson 的核心类，负责对象和 JSON 的互相转换。
         *
         * 它内部组合了 SerializerProvider、DeserializerProvider、JsonFactory 等模块。
         *
         * 默认情况下，ObjectMapper 会注册一些常见类型的序列化器，比如 StringSerializer、NumberSerializer、BeanSerializer（处理 Java Bean 的）。
         *
         * writeValueAsString(tick)
         *
         * 这个方法接收一个 Java 对象（tick），并把它转换成 JSON 格式的字符串。
         *
         * 本质是：先找到合适的序列化器（serializer），然后写入到一个 StringWriter，最后返回字符串。
         */
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(tick); // 转换成 JSON
      } catch (Exception e) {
        return null; // 脏数据丢弃
      }
    }).filter(x -> x != null);

    // 输出到 Kafka DWD topic
    KafkaUtil.producerKafka(dwdStream, "dwd_market_tick");

    env.execute("JtpSecurityOdsToDwd");
  }
}

package com.sina.security.jtp.dwd.job;

import com.sina.realtime.jtp.common.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 30777
 */
public class JtpAppSecurityEtlJob {
  public static void main(String[] args) throws Exception {
    //创建环境
    StreamExecutionEnvironment tEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    //设置并行度
    tEnv.setParallelism(1);
    //读取数据
    DataStream<String> market_data = KafkaUtil.consumerKafka(tEnv, "market_data");
    market_data.print();
    tEnv.execute("JtpAppSecurityEtlJob");

  }
}

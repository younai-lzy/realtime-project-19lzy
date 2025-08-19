package com.sina.realtime.jtp.dwd.log.job;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sina.realtime.jtp.common.utils.KafkaUtil;
import com.sina.realtime.jtp.dwd.log.function.AdJustIsNewProcessFunction;
import com.sina.realtime.jtp.dwd.log.function.AppLogSplitProcessFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
    dwd层明细数据层开发：将ODS层采集原始日志数据，进行分类处理，存储Kafka队列
    数据流向：kafka -> flink dataStream -> kafka
 * @author lzy
 */
public class JtpAppLogEtlJob {
  public static void main(String[] args) throws Exception{
    //1.创建环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //设置并行度
    env.setParallelism(1);

    //2.读取kafka数据
    DataStream<String> kafkaDataStream = KafkaUtil.consumerKafka(env, "topic-log");
    //kafkaDataStream.print("kafka");

    //3.数据转换-transformation
    DataStream<String> pageStream = processLog(kafkaDataStream);

    //4.数据输出-sink
    KafkaUtil.producerKafka(pageStream, "dwd-traffic-page-log");

    //5.启动任务 这个必须写
    env.execute("JtpAppLogEtlJob");


  }

  /**
   *  todo 数据转换
   * @param stream
   */
  static DataStream<String> processLog(DataStream<String> stream) {
    //1.数据清洗
    DataStream<String> jsonStream = appLogCleaned(stream);

    //2.新老访客修复

    DataStream<String> etlStream = processIsNew(jsonStream);

    //3.数据分流 页面日志
    DataStream<String> pageStream = splitStream(etlStream);

    return pageStream;
  }

  /**
   * 数据分流
   * @param etlStream
   * @return
   */
  private static DataStream<String> splitStream(DataStream<String> etlStream) {

    //todo 第一步，创建侧边流
    final OutputTag<String> errorTag = new OutputTag<String>("error-log-Tag"){};
    final OutputTag<String> startTag = new OutputTag<String>("start-log-Tag"){};
    final OutputTag<String> displayTag = new OutputTag<String>("display-log-Tag"){};
    final OutputTag<String> actionTag = new OutputTag<String>("action-log-Tag"){};
    //todo 第二步，分流
    SingleOutputStreamOperator<String> pageStream = etlStream.process(
      new AppLogSplitProcessFunction(errorTag, startTag, displayTag, actionTag)
    );

    //todo 第三步，将分流的数据输出
    DataStream<String> errorStream = pageStream.getSideOutput(errorTag);
    //写入Kafka
    KafkaUtil.producerKafka(errorStream, "dwd-traffic-error-log");
    DataStream<String> startStream = pageStream.getSideOutput(startTag);
    KafkaUtil.producerKafka(startStream, "dwd-traffic-start-log");
    DataStream<String> displayStream = pageStream.getSideOutput(displayTag);
    KafkaUtil.producerKafka(displayStream, "dwd-traffic-display-log");
    DataStream<String> actionStream = pageStream.getSideOutput(actionTag);
    KafkaUtil.producerKafka(actionStream, "dwd-traffic-action-log");

    return pageStream;
  }

  /**
   * 新老访客修复
   * @param
   * @return
   */
  private static DataStream<String> processIsNew(DataStream<String> stream) {
    //分组 按照mid进行分组
    KeyedStream<String, String> midStream = stream.keyBy(new KeySelector<String, String>() {
      @Override
      public String getKey(String s) throws Exception {
        // value是JSON字符串
        JSONObject jsonObject = JSON.parseObject(s);
        JSONObject commonJson = jsonObject.getJSONObject("common");
        String mid = commonJson.getString("mid");
        return mid;
      }
    });
    //2.状态编程 对is_new进行修复
    DataStream<String> isNewStream = midStream.process(new AdJustIsNewProcessFunction());

    //3.返回流
    return isNewStream;
  }

  /**
   * 清洗数据 原始App流量日志数据流
   * 将不合格数据使用侧边流输出
   * @param stream
   * @return
   */
  private static DataStream<String> appLogCleaned(DataStream<String> stream) {
    //创建错误侧输出流 输出错误数据
    final OutputTag<String> errorOutPutTag = new OutputTag<String>("errorOutPutTag"){};

    //2数据清洗
    SingleOutputStreamOperator<String> cleanStream = stream.process(new ProcessFunction<String, String>() {
      @Override
      public void processElement(String s, Context context, Collector<String> collector) throws Exception {
        //判断json数据是否有问题
        try {
          //解析json数据
          JSONObject jsonObject = JSON.parseObject(s);
          //数据合法
          collector.collect(jsonObject.toJSONString());

        }catch (Exception e) {
          //数据有问题，输出到侧边流
          context.output(errorOutPutTag, s);
        }

      }
    });

    //3.将错误数据流输出 通过主流获取
    SideOutputDataStream<String> dirtyStream = cleanStream.getSideOutput(errorOutPutTag);
    //写入Kafka
    KafkaUtil.producerKafka(dirtyStream, "dwd-traffic-dirty-log");

    //4.返回清洗后的数据流
    return cleanStream;
  }
}

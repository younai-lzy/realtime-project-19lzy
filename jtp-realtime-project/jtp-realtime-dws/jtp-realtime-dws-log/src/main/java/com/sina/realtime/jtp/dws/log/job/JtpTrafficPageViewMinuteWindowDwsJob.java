package com.sina.realtime.jtp.dws.log.job;

import com.alibaba.fastjson.JSON;
import com.sina.realtime.jtp.common.utils.DorisUtil;
import com.sina.realtime.jtp.common.utils.KafkaUtil;
import com.sina.realtime.jtp.dws.log.bean.PageViewBean;
import com.sina.realtime.jtp.dws.log.function.PageViewBeanMapFunction;
import com.sina.realtime.jtp.dws.log.function.PageViewWindowFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class JtpTrafficPageViewMinuteWindowDwsJob {
  public static void main(String[] args) throws Exception{
    //1.创建环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    //env.enableCheckpointing(3000L);
    // 2.数据源-source
    DataStream<String> pageStream = KafkaUtil.consumerKafka(env, "dwd-traffic-page-log");
    //pageStream.print("page");

    // 3.数据转换-transformation
    DataStream<String> handle = handle(pageStream);
    handle.print("result");
    // 4.数据输出-sink
    //DorisUtil.saveToDoris(handle, "jtp_traffic_page_view_minute_window", "jtp_traffic_page_view_minute_window");
    // 5.触发执行-execute
    env.execute("JtpTrafficPageViewMinuteWindowDwsJob");

  }

  private static DataStream<String> handle(DataStream<String> pageStream) {
    //s1.按照mid设备ID分组，用于计算UV，使用状态State记录今日是否第一次访问
    KeyedStream<String, String> midStream = pageStream.keyBy(
      json -> JSON.parseObject(json).getJSONObject("common").getString("mid")
    );
    //s2.将流中每条日志数据封装实体类Bean对象
    DataStream<PageViewBean> beanStream = midStream.map(new PageViewBeanMapFunction());
    //s3.事件时间字段和水位线
    DataStream<PageViewBean> timestampStream = beanStream.assignTimestampsAndWatermarks(
      WatermarkStrategy.<PageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(0))
        .withTimestampAssigner(new SerializableTimestampAssigner<PageViewBean>() {
          @Override
          public long extractTimestamp(PageViewBean pageViewBean, long recordTimestamp) {

            return pageViewBean.getTs();

            //return element.getTs() * 1000; 当时间戳不是毫秒时，需要乘以1000
          }
        })
    );

    //s4.分组keyBy: ar地区、ba品牌、ch渠道、is_new是否新用户、
    KeyedStream<PageViewBean, String> keyedStream = timestampStream.keyBy(
      all -> all.getProvince() + all.getBrand() + all.getChannel() + all.getIsNew());
    //s5.开窗: 滚动窗口、窗口大小为1分钟
    WindowedStream<PageViewBean, String, TimeWindow> windowStream = keyedStream.window(
      TumblingEventTimeWindows.of(Time.minutes(1))
    );

    //s6.聚合: 计算PV、UV、SessionCount、PageCount、PvDuringTime
    DataStream<String> resultStream = windowStream.apply(new PageViewWindowFunction());

    return resultStream;
  }
}

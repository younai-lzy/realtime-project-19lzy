package com.sina.realtime.jtp.dwd.log.function;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lzy
 * App日志分流：使用侧边流实现，划分为错误日志，启动日志，浏览日志，曝光日志，动作日志
 */
public class AppLogSplitProcessFunction extends ProcessFunction<String, String> {
  private final OutputTag<String> errorTag;
  private final OutputTag<String> startTag;
  private final OutputTag<String> displayTag;
  private final OutputTag<String> actionTag;

  public AppLogSplitProcessFunction(OutputTag<String> errorTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag,>OutputTag<String> errorTag1, OutputTag<String> startTag1, OutputTag<String> displayTag1, OutputTag<String> actionTag1) {
    this.errorTag = errorTag1;

    this.startTag = startTag1;
    this.displayTag = displayTag1;
    this.actionTag = actionTag1;
  }

  @Override
  public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {

  }
}

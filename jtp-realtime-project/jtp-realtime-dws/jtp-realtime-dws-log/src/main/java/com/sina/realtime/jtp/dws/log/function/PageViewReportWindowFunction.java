package com.sina.realtime.jtp.dws.log.function;

import com.sina.realtime.jtp.common.utils.DateTimeUtil;
import com.sina.realtime.jtp.dws.log.bean.PageViewBean;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PageViewReportWindowFunction implements WindowFunction<PageViewBean, String, String, TimeWindow> {

  /**
   * 窗口处理函数
   */
  @Override
  public void apply(String key,
                    TimeWindow window,
                    Iterable<PageViewBean> input,
                    Collector<String> out) throws Exception {
    //key表示分组字段，window表示时间窗口，可以获取窗口开始和结束时间，input表示窗口中所有数据
    //1.获取窗口开始和结束时间
    String windowStartTime = DateTimeUtil.convertLongToString(window.getStart(), "yyyy-MM-dd HH:mm:ss");
    String windowEndTime = DateTimeUtil.convertLongToString(window.getEnd(), "yyyy-MM-dd HH:mm:ss");
    //2.获取窗口中计算结果
    PageViewBean bean = input.iterator().next();
    Long pvCount = bean.getPvCount();
    Long pvDuringTime = bean.getPvDuringTime();
    Long uvCount = bean.getUvCount();
    Long sessionCount = bean.getSessionCount();
    //3.输出
    String output = windowStartTime + "," +
      windowEndTime + "," +
      windowStartTime.substring(0,10) + "," +
      key + "," +
      uvCount + "," +
      sessionCount + "," +
      pvCount + "," +
      pvDuringTime;

    out.collect(output);
  }
}

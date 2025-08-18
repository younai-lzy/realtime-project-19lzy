package com.sina.realtime.jtp.dws.log.function;

import com.sina.realtime.jtp.dws.log.bean.PageViewBean;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * @author 30777
 */
public class PageViewWindowFunction implements WindowFunction<PageViewBean, String, String, TimeWindow> {

  //格式化实例
  private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

  @Override
  public void apply(String key,
                    TimeWindow window,
                    Iterable<PageViewBean> input,
                    Collector<String> out) throws Exception {
    //1.获取窗口开始和结束时间
    String windowStartTime = format.format(window.getStart());
    String windowEndTime = format.format(window.getEnd());
    //2.对窗口中数据计算
    Long pvCount = 0L;
    Long pvDuringTime = 0L;
    Long uvCount = 0L;
    Long sessionCount = 0L;
    for (PageViewBean pageViewBean : input) {
      pvCount += pageViewBean.getPvCount();
      pvDuringTime += pageViewBean.getPvDuringTime();
      uvCount += pageViewBean.getUvCount();
      sessionCount += pageViewBean.getSessionCount();

    }

    //3.输出
    String output = windowStartTime + "," + windowEndTime + "," +
      windowStartTime.substring(0,10) + "," +
      key + "," +
      pvCount + "," +
      pvDuringTime + "," +
      uvCount + "," +
      sessionCount;
    out.collect(output);
  }
}

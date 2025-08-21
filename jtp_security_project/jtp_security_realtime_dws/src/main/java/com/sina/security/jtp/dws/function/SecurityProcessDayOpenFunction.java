//package com.sina.security.jtp.dws.function;
//
//import com.sina.security.jtp.dwd.pojo.StockData;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.util.Collector;
//
//import java.math.BigDecimal;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneOffset;
//import java.time.ZonedDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.Iterator;
//
///**
// * @author 30777
// */
//public class SecurityProcessDayOpenFunction extends ProcessWindowFunction<StockData, Tuple3<String, String, Double>, String, TimeWindow> {
//  /**
//   * 窗口处理函数
//   * key 分组字段
//   * elements 窗口中数据
//   * out 输出
//   */
//  @Override
//  public void process(String key,
//                      ProcessWindowFunction<StockData, Tuple3<String, String, Double>, String, TimeWindow>.Context context,
//                      Iterable<StockData> elements,
//                      Collector<Tuple3<String, String, Double>> out) throws Exception {
//    //数据是属于同一个key和同一个窗口的
//    Iterator<StockData> iterator = elements.iterator();
//
//    if (!iterator.hasNext()) {
//      return; //窗口为空，不输出
//    }
//    //找到窗口内时间戳最小的元素
//    StockData firstTick = iterator.next();
//    //
//    while (iterator.hasNext()) {
//      StockData next = iterator.next();
//      //获取交易日期
//      String tradingDate = next.getTradingDate();
//      String updateTime = next.getUpdateTime();
//      String ts = tradingDate + " " + updateTime;
//      //将时间转化为毫秒
//      // 2. 定义与字符串格式匹配的 DateTimeFormatter
//      // 注意：.SSS 表示毫秒，不区分大小写
//      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//
//      // 3. 将字符串解析为 LocalDateTime 对象
//      LocalDateTime localDateTime = LocalDateTime.parse(ts, formatter);
//
//      // 4. 将 LocalDateTime 转换为毫秒时间戳
//      // 需要指定时区偏移量，这里使用东八区（+8）
//      long timestampMillis = localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
//
//      //获取交易日期
//      String tradingDate2 = firstTick.getTradingDate();
//      String updateTime2 = firstTick.getUpdateTime();
//      String ts2 = tradingDate2 + " " + updateTime2;
//      //将时间转化为毫秒
//      // 2. 定义与字符串格式匹配的 DateTimeFormatter
//      // 注意：.SSS 表示毫秒，不区分大小写
//      DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
//
//      // 3. 将字符串解析为 LocalDateTime 对象
//      LocalDateTime localDateTime2 = LocalDateTime.parse(ts2, formatter2);
//
//      // 4. 将 LocalDateTime 转换为毫秒时间戳
//      // 需要指定时区偏移量，这里使用东八区（+8）
//      long timestampMillis2 = localDateTime2.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
//
//      if (timestampMillis < timestampMillis2) {
//        firstTick = next;
//      }
//
//      // 获取窗口的开始时间（作为交易日）
//      long windowStartMillis = context.window().getStart();
//      ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(windowStartMillis), ZoneOffset.ofHours(8));
//      String tradeDate = zdt.toLocalDate().toString();
//
//      // 收集结果
//      out.collect(new Tuple3<>(key, tradeDate, firstTick.getOpen()));
//    }
//  }
//}

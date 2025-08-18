package com.sina.realtime.jtp.dws.log.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sina.realtime.jtp.common.utils.DateTimeUtil;
import com.sina.realtime.jtp.dws.log.bean.PageViewBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;

import java.awt.geom.Area;
import java.util.HashMap;


/**
 * 处理
 * @author 30777
 */
public class PageViewBeanMapFunction extends RichMapFunction<String, PageViewBean> {

  /**
   *   1.声明变量
   */

  private transient ValueState<String> lastVisitState = null;
  //地区字典数据
  //private HashMap<String, String> areaDic = null;

  /**
   * 初始化
   * @param parameters
   * @throws Exception
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    //2.初始化
    lastVisitState = getRuntimeContext().getState(
      new ValueStateDescriptor<String>("lastVisitState", String.class)
    );

    //ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitState2", String.class);
    //ttl 设置
//    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
//      .updateTtlOnCreateAndWrite()
//      .neverReturnExpired()
//      .build();
//    valueStateDescriptor.enableTimeToLive(ttlConfig);
//    //2.初始化
//    lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
//    //地区字典数据初始化
//    areaDic = getAreaDic();
  }


  @Override
  public PageViewBean map(String value) throws Exception {

    // s1.解析json
    JSONObject jsonObject = JSON.parseObject(value);
    JSONObject common = jsonObject.getJSONObject("common");
    JSONObject page = jsonObject.getJSONObject("page");

    //s2.依据地区编码获取省份名称
    String arValue = common.getString("ar");
    //String province = areaDic.get(arValue);

    //UV访客数
    Long uvCount = 0L;
    //b.获取上一次访问日期
    String lastVisitDate = lastVisitState.value();
    //获取当前数据日期
    String visitDate = DateTimeUtil.convertLongToString(jsonObject.getLong("ts"), DateTimeUtil.DATE_FORMAT);
    //b-1 值为null时，表示今日第一次访问，uvCount=1
    //b-2 值不为null时，上一次访问日期 ！= 当前数据日期，也表示今日第一次访问，uvCount=1
    if (null == lastVisitDate || !lastVisitDate.equals(visitDate)) {
      uvCount = 1L;
      //更新
      lastVisitState.update(visitDate);
    }
    return new PageViewBean(
      null,
      null,
      common.getString("ba"),
      common.getString("ch"),
      arValue,
      common.getString("is_new"),
      //sv : 1 或 0 -> last_page_id is null, 否则为0
      page.get("last_page_id") == null ? 1L : 0L,
      1L,
      page.getLong("during_time"),
      uvCount,
      jsonObject.getLong("ts")
    );
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}

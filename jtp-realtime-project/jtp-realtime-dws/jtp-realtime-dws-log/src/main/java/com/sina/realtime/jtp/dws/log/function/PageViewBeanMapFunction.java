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
 * 将数据封装为实体类
 * @author 30777
 */
public class PageViewBeanMapFunction extends RichMapFunction<String, PageViewBean> {

  /**
   *   1.声明变量
   */
  private transient ValueState<String> lastVisitState = null;
  //地区字典数据
  private HashMap<String, String> areaDic = null;

  /**
   * 获取地区字典数据
   * @return
   */
  private HashMap<String, String> getAreaDic() {
    HashMap<String, String> map = new HashMap<>();
    // 添加数据
    map.put("110000","北京");
    map.put("120000","天津");
    map.put("140000","山西");
    map.put("150000","内蒙古");
    map.put("130000","河北");
    map.put("310000","上海");
    map.put("320000","江苏");
    map.put("330000","浙江");
    map.put("340000","安徽");
    map.put("350000","福建");
    map.put("360000","江西");
    map.put("370000","山东");
    map.put("710000","台湾");
    map.put("230000","黑龙江");
    map.put("220000","吉林");
    map.put("210000","辽宁");
    map.put("610000","陕西");
    map.put("620000","甘肃");
    map.put("630000","青海");
    map.put("640000","宁夏");
    map.put("650000","新疆");
    map.put("410000","河南");
    map.put("420000","湖北");
    map.put("430000","湖南");
    map.put("440000","广东");
    map.put("450000","广西");
    map.put("460000","海南");
    map.put("810000","香港");
    map.put("820000","澳门");
    map.put("510000","四川");
    map.put("520000","贵州");
    map.put("530000","云南");
    map.put("500000","重庆");
    map.put("540000","西藏");
    // 返回集合
    return map ;
  }

  /**
   * 初始化
   * @param parameters
   * @throws Exception
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit-date", String.class);

    //2.初始化
    lastVisitState = getRuntimeContext().getState(stateDescriptor);

    //TTL 设置
    StateTtlConfig ttlConfig = StateTtlConfig
      .newBuilder(Time.days(1))
      .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
      .neverReturnExpired()
      .build();
    //设置启用
    stateDescriptor.enableTimeToLive(ttlConfig);
    //地区字典数据初始化
    areaDic = getAreaDic();
  }


  @Override
  public PageViewBean map(String value) throws Exception {

    // s1.解析json
    JSONObject jsonObject = JSON.parseObject(value);
    JSONObject common = jsonObject.getJSONObject("common");
    JSONObject page = jsonObject.getJSONObject("page");

    //s2.依据地区编码获取省份名称
    String arValue = common.getString("ar");
    String province = areaDic.get(arValue);

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
      province,
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

package com.sina.realtime.jtp.dwd.log.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sina.realtime.jtp.common.utils.DateTimeUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 需求：利用状态编程实现AdJustIsNewProcessFunction
 */
public class AdJustIsNewProcessFunction extends KeyedProcessFunction<String, String, String> {

  //声明状态
  private transient ValueState<String> firstDateState = null;

  @Override
  public void open(Configuration parameters) throws Exception {
    //2.初始化状态
    firstDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstDateState", String.class));
  }

  @Override
  public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
    //获取状态值
    String firstDate = firstDateState.value();

    //解析字段值
    JSONObject jsonObject = JSON.parseObject(value);
    // todo 获取is_new的值
    String isNewValue = jsonObject.getJSONObject("common").getString("is_new");
    //获取时间戳
    Long tsValue = jsonObject.getLong("ts");

    //判断isNew 是否为1 firstDate = null , firstDate != null
    if ("1".equals(isNewValue)) {

      if (null == firstDate) {
        //说明是第一次进入
        //更新状态值
        //此时is_new正确
        firstDateState.update(DateTimeUtil.convertLongToString(tsValue, DateTimeUtil.DATE_FORMAT));
      }//说明不是第一次进入
      else {
        //状态值等于数据日期，不做处理
        if (!firstDate.equals(DateTimeUtil.convertLongToString(tsValue, DateTimeUtil.DATE_FORMAT))) {
          //此时is_new错误, 需要修正, 设置为0
          jsonObject.getJSONObject("common").put("is_new", "0");
        }
      }
    } else {
      // 当用户早早登录访问，当时实时应用后来处理，用户第一次访问时，firstDate为null
      if (null == firstDate) {
        //设置一个昨天日期即可
        firstDateState.update(DateTimeUtil.convertLongToString(tsValue - 86400000, DateTimeUtil.DATE_FORMAT));
      }

    }
    //收集数据
    out.collect(jsonObject.toJSONString());
  }
}

package com.sina.realtime.jtp.dwd.log.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lzy
 * App日志分流：使用侧边流实现，划分为错误日志，启动日志，浏览日志，曝光日志，动作日志
 */
public class AppLogSplitProcessFunction2 extends ProcessFunction<String, String> {
  private final OutputTag<String> errorTag;
  private final OutputTag<String> startTag;
  private final OutputTag<String> displayTag;
  private final OutputTag<String> actionTag;


  public AppLogSplitProcessFunction2(OutputTag<String> errorTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
    this.errorTag = errorTag;
    this.startTag = startTag;
    this.displayTag = displayTag;
    this.actionTag = actionTag;
  }

  /**
   *
   * @param value The input value.
   * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
   *
   * @param out The collector for returning result values.
   * @throws Exception
   *
   *
   */
  @Override
  public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
    //第一步、解析JSON数据
    JSONObject jsonObject = JSON.parseObject(value);
    //第二部、错误日志 获取错误日志
    if(null != jsonObject.get("err")) {
      //侧边流输出
      ctx.output(errorTag, value);
      //错误数据已经单独保存，无需再保存，直接删除
      jsonObject.remove("err");
    }
    //启动日志 获取启动日志
    if (null != jsonObject.get("start")) {
      //侧边流输出
      ctx.output(startTag, jsonObject.toJSONString());
    }
    //页面日志
    else {
      //输出页面日志
      out.collect(jsonObject.toJSONString());
      JSONArray displayJsonArray = jsonObject.getJSONArray("displays");
      JSONArray actionsJson = jsonObject.getJSONArray("actions");

      //删除曝光和动作
      jsonObject.remove("displays");
      jsonObject.remove("actions");
      //第五步、曝光日志
      if (null != displayJsonArray && !displayJsonArray.isEmpty()) {
        for (int i = 0; i < displayJsonArray.size(); i++) {
          JSONObject displayJsonObject = displayJsonArray.getJSONObject(i);
          //添加字段值
          jsonObject.put("display", displayJsonObject);
          //输出
          ctx.output(displayTag, jsonObject.toJSONString());
        }
      }
      //将上述添加曝光数据字段值删除
      jsonObject.remove("display");

      //第六步、动作日志
      if (null != actionsJson && !actionsJson.isEmpty()) {
        for (int i = 0; i < actionsJson.size(); i++) {
          JSONObject actionJsonObject = actionsJson.getJSONObject(i);
          //添加字段值
          jsonObject.put("action", actionJsonObject);
          //输出
          ctx.output(actionTag, jsonObject.toJSONString());
        }
      }
    }
  }
}

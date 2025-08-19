package com.sina.realtime.jtp.dws.log.function;

import com.sina.realtime.jtp.dws.log.bean.PageViewBean;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * 创建ReduceFunction函数实例，对每分钟窗口中数据进行增量计算
 * ReduceFunction函数要求集合结果类型，必须与窗口中数据类型一致
 *
 * @author 30777
 */
public class PageViewReportReduceFunction implements ReduceFunction<PageViewBean> {
  @Override
  public PageViewBean reduce(PageViewBean tmp, PageViewBean bean) throws Exception {
    /*
    * tmp : 表示增量计算中间结果：bean : 表示窗口中每条数据
    * */
    //1.增量计算：各个指标值相加
    tmp.setPvCount(tmp.getPvCount() + bean.getPvCount());
    tmp.setPvDuringTime(tmp.getPvDuringTime() + bean.getPvDuringTime());
    tmp.setUvCount(tmp.getUvCount() + bean.getUvCount());
    tmp.setSessionCount(tmp.getSessionCount() + bean.getSessionCount());
    //2.返回结果
    return tmp;
  }
}

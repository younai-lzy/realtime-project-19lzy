package com.sina.realtime.jtp.dws.log.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageViewBean {
  /**
   * 时间维度：窗口开始时间和窗口结束时间
   */
  private String windowStartTime;
  private String windowEndTime;
  /**
   * 粒度：不同维度，品牌ba，渠道ch，地区ar，访客类别is_new(是否为新用户)
   */
  private String brand;
  private String channel;
  private String province;
  private String isNew;
  /**
   * 度量值：会话数、页面浏览数、浏览总时长、独立访客数
   */
  private Long sessionCount;
  private Long pvCount;
  private Long pvDuringTime;
  private Long uvCount;
  /**
   * 时间戳
   */
  private Long ts;
}

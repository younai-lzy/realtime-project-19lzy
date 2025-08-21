package com.sina.security.jtp.dwd.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author 30777
 *
 * 描述：股票数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockData {

  public String tradingDate; // 第0列: 交易日期
  public String code;       // 第1列: M001
  public String stockCode; // 第2列: 股票编号
  public String stockName; // 第3列: 股票名称
  public Long volume;      // 第4列: 成交量（单位：股 / 手，通常是累计成交量）
  public BigDecimal changePercent; //第5列: 成交额（单位：元，累计成交金额）
  public Double openPrice;       // 第6列: 开盘价（当天第一个成交价格）
  public Double highPrice;  // 第7列: 最高价（当日盘中最高成交价）
  public Double maxPrice;   // 第8列：最高价（表示历史/阶段最高价）
  public Double minPrice;   // 第9列: 最低价（当日盘中最低成交价）
  public Double latestPrice;// 第10列: 最新价（最近一笔成交价，即实时价格）
  public Double prevClose;  // 第11列: 昨收价（上一交易日的收盘价，用于计算涨跌幅）
  public String typeFlag;   // 第12列：数据类型标识（如 T0 表示当日实时数据，T1 表示昨日数据等）
  public String updateTime; // 第13列：更新时间（行情时间戳，例如 HH:MM:SS.mmm）


}

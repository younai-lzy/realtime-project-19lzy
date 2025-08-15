package com.sina.utils;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.FastDateFormat;

/**
 * 日期时间转换工具类
 * @author xuanyu
 */
public class DateTimeUtil {

	/**
	 * 日期格式
	 */
	public static final String DATE_FORMAT = "yyyy-MM-dd" ;
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss" ;

	/**
	 * 将Long类型时间戳转换为字符串类型日期时间格式
	 * @param timestamp long类型时间戳
	 * @param format 日期时间格式，比如yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss
	 */
	public static String convertLongToString(Long timestamp, String format){
		// Format对象
		FastDateFormat fastDateFormat = FastDateFormat.getInstance(format);
		// 转换，返回
		return fastDateFormat.format(timestamp);
	}

	/**
	 * 将字符串类型日期时间数据解析转换为Long类型时间戳
	 * @param datetime 日期时间格式数据
	 * @param format 日期时间格式，比如yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss
	 */
	@SneakyThrows
	public static Long convertStringToLong(String datetime, String format){
		// Format对象
		FastDateFormat fastDateFormat = FastDateFormat.getInstance(format);
		// 解析，返回
		return fastDateFormat.parse(datetime).getTime();
	}

	public static void main(String[] args) {
		System.out.println(convertLongToString(1721214848000L, DATE_FORMAT));
	}

}

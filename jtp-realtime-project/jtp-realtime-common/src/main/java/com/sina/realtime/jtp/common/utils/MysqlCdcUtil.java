package com.sina.realtime.jtp.common.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Flink CDC实时捕获Mysql数据库表中数据
 * @author xuanyun
 */
public class MysqlCdcUtil {

	/**
	 * Flink CDC 读取数据时，没有特殊设置反序列化，及针对Decimal类型和DateTime类型数据。
	 */
	public static DataStream<String> cdcMysqlRaw(StreamExecutionEnvironment env, String database, String table){
		// a. 数据源
		MySqlSource<String> mysqlsource = MySqlSource.<String>builder()
			.hostname("node101")
			.port(3306)
			.databaseList(database)
			.tableList(database + "." + table)
			.username("root")
			.password("123456")
			.serverId("5401")
			.serverTimeZone("Asia/Shanghai")
			.startupOptions(StartupOptions.earliest())
			.deserializer(new JsonDebeziumDeserializationSchema())
			.build();
		// b. 读取数据
		DataStreamSource<String> stream = env.fromSource(
			mysqlsource, WatermarkStrategy.noWatermarks(), "MysqlSource"
		);
		// c. 返回
		return stream;
	}

	/**
	 * 使用Flink CDC方式，拉取Mysql表数据，从最新offset偏移量读取数据
	 * @param env 流式执行环境
	 * @param database 数据库名称
	 * @param table 表名称
	 * @return 数据流，数据类型为json字符串
	 */
	public static DataStream<String> cdcMysqlDeser(StreamExecutionEnvironment env, String database, String table){
		// a. 反序列化：DECIMAL类型数据使用NUMERIC数值转换
		Map<String, Object> configs = new HashMap<>();
		configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
		JsonDebeziumDeserializationSchema schema = new JsonDebeziumDeserializationSchema(false, configs);
		// b. 数据源
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
			.hostname("node101")
			.port(3306)
			.databaseList(database)
			.tableList(database + "." + table)
			.username("root")
			.password("123456")
			.serverId("5401")
			.serverTimeZone("Asia/Shanghai")
			.startupOptions(StartupOptions.earliest())
			.debeziumProperties(getDebeziumProperties())
			.deserializer(schema)
			.build();
		// c. 读取数据
		DataStreamSource<String> stream = env.fromSource(
			mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source"
		);
		// d. 返回
		return stream;
	}

	/**
	 * 使用Flink CDC方式，拉取Mysql表数据，从最新offset偏移量读取数据
	 * @param env 流式执行环境
	 * @param database 数据库名称
	 * @param tableList 表名称，可以传递多个
	 * @return 数据流，数据类型为json字符串
	 */
	public static DataStream<String> cdcMysqlEarliest(StreamExecutionEnvironment env, String database, String... tableList){
		// a. 反序列化
		Map<String, Object> configs = new HashMap<>();
		configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
		JsonDebeziumDeserializationSchema schema = new JsonDebeziumDeserializationSchema(false, configs);

		StringBuffer buffer = new StringBuffer();
		for (String table : tableList) {
			buffer.append(database).append(".").append(table).append(",");
		}
		buffer = buffer.deleteCharAt(buffer.length() - 1);

		// b. 数据源
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
			.hostname("node101")
			.port(3306)
			.databaseList(database)
			.tableList(buffer.toString())
			.username("root")
			.password("123456")
			.serverId("5401")
			.serverTimeZone("Asia/Shanghai")
			.startupOptions(StartupOptions.earliest())
			.debeziumProperties(getDebeziumProperties())
			.deserializer(schema)
			.build();
		// c. 读取数据
		DataStreamSource<String> stream = env.fromSource(
			mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlEarliestSource"
		);
		// d. 返回
		return stream;
	}

	/**
	 * 使用Flink CDC方式，拉取Mysql表数据，从binlog中最早offset偏移量读取数据
	 * @param env 流式执行环境
	 * @param database 数据库名称
	 * @return 数据流，数据类型为json字符串
	 */
	public static DataStream<String> cdcMysqlInitial(StreamExecutionEnvironment env, String database, String... tableList){
		// a. 反序列化
		Map<String, Object> configs = new HashMap<>();
		configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
		JsonDebeziumDeserializationSchema schema = new JsonDebeziumDeserializationSchema(false, configs);

		StringBuffer buffer = new StringBuffer();
		for (String table : tableList) {
			buffer.append(database).append(".").append(table).append(",");
		}
		buffer = buffer.deleteCharAt(buffer.length() - 1);

		// b. 数据源
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
			.hostname("node101")
			.port(3306)
			.databaseList(database)
			.tableList(buffer.toString())
			.username("root")
			.password("123456")
			.serverId("5402")
			.serverTimeZone("Asia/Shanghai")
			.startupOptions(StartupOptions.initial())
			.debeziumProperties(getDebeziumProperties())
			.deserializer(schema)
			.build();
		// c. 读取数据
		DataStreamSource<String> stream = env.fromSource(
			mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlLInitialSource"
		);
		// d. 返回
		return stream;
	}


	/**
	 * 使用Flink CDC方式，拉取Mysql表数据，从最新offset偏移量读取数据
	 * @param env 流式执行环境
	 * @param database 数据库名称
	 * @return 数据流，数据类型为json字符串
	 */
	public static DataStream<String> cdcMysql(StreamExecutionEnvironment env, String database){
		// a. 反序列化
		Map<String, Object> configs = new HashMap<>();
		configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
		JsonDebeziumDeserializationSchema schema = new JsonDebeziumDeserializationSchema(false, configs);
		// b. 数据源
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
			.hostname("node101")
			.port(3306)
			.databaseList(database)
			.tableList()
			.username("root")
			.password("123456")
			.serverId("5403")
			.serverTimeZone("Asia/Shanghai")
			.startupOptions(StartupOptions.latest())
			.debeziumProperties(getDebeziumProperties())
			.deserializer(schema)
			.build();
		// c. 读取数据
		DataStreamSource<String> stream = env.fromSource(
			mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlLatestSource"
		);
		// d. 返回
		return stream;
	}


	/**
	 * 使用Flink CDC方式，拉取Mysql表数据，从最新offset偏移量读取数据
	 * @param env 流式执行环境
	 * @param database 数据库名称
	 * @param tableList 表名称，可以传递多个
	 * @return 数据流，数据类型为json字符串
	 */
	public static DataStream<String> cdcMysql(StreamExecutionEnvironment env, String database, String... tableList){
		// a. 反序列化
		Map<String, Object> configs = new HashMap<>();
		configs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
		JsonDebeziumDeserializationSchema schema = new JsonDebeziumDeserializationSchema(false, configs);

		StringBuffer buffer = new StringBuffer();
		for (String table : tableList) {
			buffer.append(database).append(".").append(table).append(",");
		}
		buffer = buffer.deleteCharAt(buffer.length() - 1);

		// b. 数据源
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
			.hostname("node101")
			.port(3306)
			.databaseList(database)
			.tableList(buffer.toString())
			.username("root")
			.password("123456")
			.serverId("5403")
			.serverTimeZone("Asia/Shanghai")
			.startupOptions(StartupOptions.latest())
			.debeziumProperties(getDebeziumProperties())
			.deserializer(schema)
			.build();
		// c. 读取数据
		DataStreamSource<String> stream = env.fromSource(
			mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source"
		);
		// d. 返回
		return stream;
	}

	private static Properties getDebeziumProperties(){
		Properties properties = new Properties();
		properties.setProperty("converters", "dateConverters");
		properties.setProperty("dateConverters.type", MySqlDateTimeConverter.class.getName());
		properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
		properties.setProperty("dateConverters.format.time", "HH:mm:ss");
		properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
		properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
		properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
		return properties;
	}

	/**
	 * 自定义时间转换配置。
	 */
	public static class MySqlDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
		private final static Logger logger = LoggerFactory.getLogger(MySqlDateTimeConverter.class);

		private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
		private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
		private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
		private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;
		private ZoneId timestampZoneId = ZoneId.systemDefault();

		@Override
		public void configure(Properties props) {
			readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
			readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
			readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
			readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
			readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
		}

		private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
			String settingValue = (String) properties.get(settingKey);
			if (settingValue == null || settingValue.isEmpty()) {
				return;
			}
			try {
				callback.accept(settingValue.trim());
			} catch (IllegalArgumentException | DateTimeException e) {
				logger.error("The {} setting is illegal: {}",settingKey,settingValue);
				throw e;
			}
		}

		@Override
		public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
			String sqlType = column.typeName().toUpperCase();
			SchemaBuilder schemaBuilder = null;
			Converter converter = null;
			if ("DATE".equals(sqlType)) {
				schemaBuilder = SchemaBuilder.string().optional().name("debezium.date.string");
				converter = this::convertDate;
			}
			if ("TIME".equals(sqlType)) {
				schemaBuilder = SchemaBuilder.string().optional().name("debezium.date.string");
				converter = this::convertTime;
			}
			if ("DATETIME".equals(sqlType)) {
				schemaBuilder = SchemaBuilder.string().optional().name("debezium.date.string");
				converter = this::convertDateTime;
			}
			if ("TIMESTAMP".equals(sqlType)) {
				schemaBuilder = SchemaBuilder.string().optional().name("debezium.date.string");
				converter = this::convertTimestamp;
			}
			if (schemaBuilder != null) {
				registration.register(schemaBuilder, converter);
			}
		}

		private String convertDate(Object input) {
			if (input == null) return null;

			if (input instanceof LocalDate) {
				return dateFormatter.format((LocalDate) input);
			}

			if (input instanceof Integer) {
				LocalDate date = LocalDate.ofEpochDay((Integer) input);
				return dateFormatter.format(date);
			}

			return String.valueOf(input);
		}

		private String convertTime(Object input) {
			if (input == null) return null;

			if (input instanceof Duration) {
				Duration duration = (Duration) input;
				long seconds = duration.getSeconds();
				int nano = duration.getNano();
				LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
				return timeFormatter.format(time);
			}

			return String.valueOf(input);
		}

		private String convertDateTime(Object input) {
			if (input == null) return null;

			if (input instanceof LocalDateTime) {
				return datetimeFormatter.format((LocalDateTime) input).replaceAll("T", " ");
			}

			return String.valueOf(input);
		}

		private String convertTimestamp(Object input) {
			if (input == null) return null;

			if (input instanceof ZonedDateTime) {
				// mysql的timestamp会转成UTC存储，这里的zonedDatetime都是UTC时间
				ZonedDateTime zonedDateTime = (ZonedDateTime) input;
				LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
				return timestampFormatter.format(localDateTime).replaceAll("T", " ");
			}

			return String.valueOf(input);
		}
	}

}



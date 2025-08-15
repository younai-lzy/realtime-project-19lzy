package com.sina.realtime.jtp.common.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Flink 从Kafka主题Topic中读写数据封装工具类
 * @author xuanyun
 */
public class KafkaUtil {

	/**
	 * 从Kafka Topic队列实时消费数据，从最小偏移量开始消费
	 * @param env 流式执行环境
	 * @param topic 队列名称
	 * @return 数据流
	 */
	public static DataStream<String> consumerKafka(StreamExecutionEnvironment env, String topic){
		// a. 数据源
		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers("node101:9092,node102:9092,node103:9092")
			.setTopics(topic)
			.setGroupId("gid-" + topic)
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();
		// b. 数据流
		DataStreamSource<String> stream = env.fromSource(
			source, WatermarkStrategy.noWatermarks(), "Kafka Source"
		);
		// c. 返回
		return stream ;
	}

	/**
	 * 从Kafka Topic队列实时消费数据，从最新偏移量开始消费
	 * @param env 流式执行环境
	 * @param topic 队列名称
	 * @return 数据流
	 */
	public static DataStream<String> consumerKafkaFromLatest(StreamExecutionEnvironment env, String topic){
		// a. 数据源
		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers("node101:9092,node102:9092,node103:9092")
			.setTopics(topic)
			.setGroupId("g-" + topic)
			.setStartingOffsets(OffsetsInitializer.latest())
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();
		// b. 数据流
		DataStreamSource<String> stream = env.fromSource(
			source, WatermarkStrategy.noWatermarks(), "Kafka Source"
		);
		// c. 返回
		return stream ;
	}

	/**
	 * 将DataStream数据流中数据实时写入Kafka消息队列，要求数据类型String
	 * @param stream 数据流
	 * @param topic 主题名称
	 */
	public static void producerKafka(DataStream<String> stream, String topic){
		// a. 属性
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "node101:9092,node102:9092,node103:9092");
		//默认情况下，FlinkKafkaProducer 将 producer config 中的 transaction.timeout.ms 属性设置为 1 小时
		properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000);
		// b. 序列化
		KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
			@Override
			public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
				return new ProducerRecord<>(
					topic, element.getBytes(StandardCharsets.UTF_8)
				);
			}
		};
		// c. 生产者
		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
			topic,
			serializationSchema,
			properties,
			FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);
		// d. 保存
		stream.addSink(myProducer);
	}
	
}

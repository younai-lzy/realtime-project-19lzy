package com.sina.security.jtp.common.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Properties;

/**
 * 将数据保存Doris表中，提供相关工具类
 * @author xuanyu
 */
public class DorisUtil {

    /**
     * 将DataStream数据流数据保存Doris表中，要求数据为JSON字符串或CSV字符串
     *      https://doris.apache.org/zh-CN/docs/ecosystem/flink-doris-connector/#%E4%BD%BF%E7%94%A8-datastream-api-%E5%86%99%E5%85%A5%E6%95%B0%E6%8D%AE
     * @param stream 数据流
     * @param database 数据库名称
     * @param table 表名称
     */
    public static void saveToDoris(DataStream<String> stream, String database, String table) {

        // s1-Doris数据库连接信息
        DorisOptions options = DorisOptions.builder()
            .setFenodes("node102:8030")
            .setTableIdentifier(database + "." + table)
            .setUsername("root")
            .setPassword("123456")
            .build();

        // s2-Doris插入数据时执行参数
        Properties props = new Properties();
        props.setProperty("format", "csv");
        props.setProperty("column_separator", ",");
        // props.setProperty("format", "json");
        // props.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
            // stream-load 导入数据时 label 的前缀
            .setLabelPrefix("label_doris" )
            // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
            .disable2PC()
            // 批次条数: 默认 3
            .setBufferCount(3)
            // 批次大小: 默认 1M
            .setBufferSize(1024 * 1024)
            // 批次输出间隔  上述三个批次的限制条件是或的关系
            .setCheckInterval(3000)
            .setMaxRetries(3)
            // 设置 stream load 的数据格式 默认是 csv,需要改成 json
            .setStreamLoadProp(props)
            .build();

        // s3-构建Sink对象
        DorisSink<String> sink = DorisSink.<String>builder()
            .setDorisReadOptions(DorisReadOptions.builder().build())
            // 设置 doris 连接参数
            .setDorisOptions(options)
            // 设置doris 执行参数
            .setDorisExecutionOptions(executionOptions)
            // 指定序列化方式
            .setSerializer(new SimpleStringSerializer())
            .build();

        // s4-数据写入表中
        stream.sinkTo(sink) ;
    }

}
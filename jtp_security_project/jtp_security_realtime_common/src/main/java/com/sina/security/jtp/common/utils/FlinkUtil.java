package com.sina.security.jtp.common.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Job 工具类，比如构建流式执行环境和表执行环境实例
 * @author xuanyu
 * @date 2025/5/28
 */
public class FlinkUtil {

	/**
	 * 构建Flink DataStream流式应用程序执行环境，合理设置参数，包括Checkpoint检查点和重启策略
	 * @param port 应用端口号
	 * @param parallelism 并行度
	 * @param ckpt 保存路径
	 * @return 返回流式执行环境
	 */
	public static StreamExecutionEnvironment getStreamEnv(int parallelism, int port, String ckpt){
		// 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户
		System.setProperty("HADOOP_USER_NAME", "bwie");

		// 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
		Configuration conf = new Configuration();
		conf.setInteger("rest.port", port);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

		// 1.3 设置并行度
		env.setParallelism(parallelism);

		// 1.4 状态后端及检查点相关配置
		// 1.4.1 设置状态后端，内存状态后端MemoryStateBackend
		env.setStateBackend(new HashMapStateBackend());

		// 1.4.2 开启 checkpoint
		env.enableCheckpointing(5000);
		// 1.4.3 设置 checkpoint 模式: 精准一次
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// 1.4.4 checkpoint 存储
		env.getCheckpointConfig().setCheckpointStorage("hdfs://node101:8020/jtp-realtime/" + ckpt);
		// 1.4.5 checkpoint 并发数
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// 1.4.6 checkpoint 之间的最小间隔
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
		// 1.4.7 checkpoint  的超时时间
		env.getCheckpointConfig().setCheckpointTimeout(10000);
		// 1.4.8 job 取消时 checkpoint 保留策略
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		// 1.5 重启策略
		env.setRestartStrategy(
			// 设置重启策略，最多失败重启3次，每次间隔10s
			RestartStrategies.fixedDelayRestart(3, 10000L)
		);

		return env ;
	}


	/**
	 * 构建Flink SQL 表执行环境，合理设置参数，包括Checkpoint检查点和重启策略
	 */
	public static TableEnvironment getTableEnv(String jobName, int parallelism){
		// 1-环境属性设置
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.build();
		TableEnvironment tabEnv = TableEnvironment.create(settings) ;

		// 2-配置属性设置
		Configuration configuration = tabEnv.getConfig().getConfiguration();
		// 设置Job名称
		configuration.setString("pipeline.name", jobName);
		// 设置时区
		configuration.setString("table.local-time-zone", "Asia/Shanghai");
		// 启用Checkpoint
		configuration.setString("execution.checkpointing.interval", "60000");
		configuration.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
		configuration.setString("execution.checkpointing.timeout", "600000");
		configuration.setString("execution.checkpointing.max-concurrent-checkpoints", "1");
		// 设置状态存储
		configuration.setString("state.backend", "rocksdb");
		configuration.setBoolean("state.backend.incremental", true);
		configuration.setString("execution.savepoint.path", "hdfs://node101:8020/jtp-realtime/" + jobName);

		// 设置全局并行度
		configuration.setInteger("parallelism.default", parallelism);
		// 优化：State TTL 时间
		configuration.setString("table.exec.state.ttl", "5 s");

		// 3-返回对象
		return tabEnv;
	}

}

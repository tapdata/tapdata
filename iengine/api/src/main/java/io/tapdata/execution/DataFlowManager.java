package io.tapdata.execution;

import com.tapdata.entity.dataflow.DataFlow;

/**
 * 负责处理与data flow相关的所有操作
 * 包括：启动、停止、读取flow所有runtime信息
 *
 * @author jackin
 * @date 2021/3/9 11:56 AM
 **/
public interface DataFlowManager {

	/**
	 * 将data flow提交到任务调度引擎
	 *
	 * @param dataFlow data flow 配置
	 * @return 返回data flow操作代理
	 */
	DataFlowClient submit(DataFlow dataFlow);

	boolean stop(DataFlowClient dataFlowClient, boolean force);

	/**
	 * 任务错误处理
	 *
	 * @param dataFlowClient
	 * @return true: There is an exception, the task is stopped, the external cache needs to be cleaned
	 * false: No exception, no processing required
	 */
	default boolean error(DataFlowClient dataFlowClient) {
		return false;
	}

	/**
	 * 刷新内存信息
	 */
	default void refreshCache(DataFlowClient dataFlowClient) {

	}

	default void ping(DataFlowClient dataFlowClient) {

	}
}

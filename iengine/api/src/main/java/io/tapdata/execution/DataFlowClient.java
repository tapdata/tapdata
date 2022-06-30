package io.tapdata.execution;

import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.DataFlowStats;
import io.tapdata.common.DataFlowStatus;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * 提交data flow后返回的data flow 操作代理类
 *
 * @author jackin
 * @date 2021/3/9 4:29 PM
 **/
public interface DataFlowClient {

	/**
	 * @return 返回data flow
	 */
	DataFlow getDataFlow();

	/**
	 * 取消已提交的任务
	 *
	 * @return
	 */
	CompletableFuture<Void> cancel(boolean force);

	/**
	 * 将队列中的数据消费完后再停止任务
	 *
	 * @param name
	 * @return
	 */
	CompletableFuture<Void> stopWithSavepoint(String name);

	/**
	 * 将队列中的数据消费完后再停止任务
	 *
	 * @param name directory the savepoint should be written to
	 * @return
	 */
	CompletableFuture<Void> triggerSavepoint(String name);

	/**
	 * Returns the time when the job was submitted to the cluster.
	 * <p>
	 * The time is assigned by reading {@code System.currentTimeMillis()} of
	 * the master member that executes the job for the first time. It doesn't
	 * change on restart.
	 */
	long getSubmissionTime();

	/**
	 * Returns the current status of this job.
	 */
	@Nonnull
	DataFlowStatus getStatus();

	/**
	 * 获取data flow统计信息
	 *
	 * @return
	 */
	void dataflowStats(DataFlowStats stats);
}

package io.tapdata.flow.engine.V2.monitor.impl;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.AppType;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-03-08 18:53
 **/
public class TaskPingTimeMonitor extends TaskMonitor<Object> {
	private static final Logger logger = LogManager.getLogger(TaskPingTimeMonitor.class);

	private final static long PING_INTERVAL_MS = 5000L;

	private ScheduledExecutorService executorService;
	private HttpClientMongoOperator clientMongoOperator;
	private Supplier<Boolean> stopTask;

	public TaskPingTimeMonitor(TaskDto taskDto, HttpClientMongoOperator clientMongoOperator, SupplierImpl<Boolean> stopTask) {
		super(taskDto);
		this.executorService = new ScheduledThreadPoolExecutor(1);
		this.clientMongoOperator = clientMongoOperator;
		this.stopTask = stopTask;
	}

	@Override
	public void start() {
		// use scheduleWithFixedDelay because it is not need execute lost times
		executorService.scheduleWithFixedDelay(
				() -> {
					try {
						Thread.currentThread().setName("Task-PingTime-" + taskDto.getId().toHexString());
						Log4jUtil.setThreadContext(taskDto);

						UpdateResult update = clientMongoOperator.update(
								new Query(where("_id").is(taskDto.getId()).and("status").nin(TaskDto.STATUS_ERROR, TaskDto.STATUS_SCHEDULE_FAILED)),
								new Update().set("pingTime", System.currentTimeMillis()),
								ConnectorConstant.TASK_COLLECTION
						);

						// 任务状态异常，应该将任务停止
						if (update.getModifiedCount() == 0) {
							logger.warn("Send task ping time failed, will stop task.");
							stopTask.get();
						}
					} catch (Exception e) {
						logger.warn("Send task ping time failed, will stop task: {}", e.getMessage(), e);
						if (!AppType.init().isCloud()) {
							stopTask.get();
						}
					} finally {
						ThreadContext.clearAll();
					}
				}, 0L, PING_INTERVAL_MS, TimeUnit.MILLISECONDS
		);
	}

	@Override
	public void close() throws IOException {
		ExecutorUtil.shutdown(executorService, 5L, TimeUnit.SECONDS);
	}
}

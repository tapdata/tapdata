package io.tapdata.flow.engine.V2.monitor.impl;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

	public TaskPingTimeMonitor(TaskDto taskDto, HttpClientMongoOperator clientMongoOperator) {
		super(taskDto);
		this.executorService = new ScheduledThreadPoolExecutor(1);
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void start() {
		// use scheduleWithFixedDelay because it is not need execute lost times
		executorService.scheduleWithFixedDelay(
				() -> {
					try {
						clientMongoOperator.update(
								new Query(where("_id").is(taskDto.getId())),
								new Update().set("pingTime", System.currentTimeMillis()),
								ConnectorConstant.TASK_COLLECTION
						);
					} catch (Exception e) {
						Thread.currentThread().setName("Task-PingTime-" + taskDto.getId().toHexString());
						Log4jUtil.setThreadContext(taskDto);
						logger.warn("Send task ping time failed: {}", e.getMessage(), e);
					}
				}, 0L, PING_INTERVAL_MS, TimeUnit.MILLISECONDS
		);
	}

	@Override
	public void close() throws IOException {
		ExecutorUtil.shutdown(executorService, 5L, TimeUnit.SECONDS);
	}
}

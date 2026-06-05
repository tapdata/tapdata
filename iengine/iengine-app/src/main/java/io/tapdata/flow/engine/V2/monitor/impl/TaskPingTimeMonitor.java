package io.tapdata.flow.engine.V2.monitor.impl;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.flow.engine.V2.util.ConsumerImpl;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.utils.AppType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author samuel
 * @Description
 * @create 2022-03-08 18:53
 **/
public class TaskPingTimeMonitor extends TaskMonitor<Object> {
	private final static long PING_INTERVAL_MS = 5000L;
	private static final long DEFAULT_TASK_HEART_TIMEOUT_MS = 60000L;
	public static final String TAG = TaskPingTimeMonitor.class.getSimpleName();

	private Logger logger = LogManager.getLogger(TaskPingTimeMonitor.class);
	private final ScheduledExecutorService executorService;
	private final HttpClientMongoOperator clientMongoOperator;
	private final Supplier<Boolean> stopTask;
	private final Consumer<TerminalMode> taskMonitor;
    private long latestSuccessPingTime = 0;

	public TaskPingTimeMonitor(TaskDto taskDto, HttpClientMongoOperator clientMongoOperator, SupplierImpl<Boolean> stopTask, ConsumerImpl<TerminalMode> terminalMode) {
		super(taskDto);
		this.executorService = new ScheduledThreadPoolExecutor(1);
		this.clientMongoOperator = clientMongoOperator;
		this.stopTask = stopTask;
		this.taskMonitor = terminalMode;
	}

	@Override
	public void start() {
		latestSuccessPingTime = System.currentTimeMillis();
		// Always ping every PING_INTERVAL_MS (5s). TM treats pingTime older than
		// JOB_HEART_TIMEOUT (≥25s) as a dead task; gating ping by heartExpire/2
		// previously made the ping interval equal the timeout, leaving zero buffer
		// for transient network/GC hiccups and triggering false-positive HA failover.
		executorService.scheduleWithFixedDelay(
				() -> {
					Thread.currentThread().setName("Task-PingTime-" + taskDto.getId().toHexString());

					long now = System.currentTimeMillis();
					Query query = new Query(where("_id").is(taskDto.getId())
							.and("status").nin(TaskDto.STATUS_ERROR, TaskDto.STATUS_SCHEDULE_FAILED)
							.and("agentId").is(taskDto.getAgentId())
					);
					Update update = new Update().set("pingTime", now);
					try {
						taskPingTimeUseHttp(query, update);
					} catch (Exception e) {
						logger.warn(e.getMessage(), e);
					} finally {
						ThreadContext.clearAll();
					}
				}, 0L, PING_INTERVAL_MS, TimeUnit.MILLISECONDS
		);
	}

	@Override
	public void close() throws IOException {
		CommonUtils.ignoreAnyError(() -> Optional.ofNullable(executorService).ifPresent(ExecutorService::shutdownNow), TAG);
	}

	public void taskPingTimeUseHttp(Query query, Update update) {
		boolean isCloud = AppType.currentType().isCloud();
		try {
			UpdateResult updateResult = clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
			if (updateResult.getModifiedCount() != 0) {
				latestSuccessPingTime = System.currentTimeMillis();
				return;
			}
			if (isCloud) {
				logger.warn("TaskHA event=ping_rejected reason=update_modifiedCount_zero, cloud task will keep running");
				return;
			}
			logger.warn("TaskHA event=ping_rejected reason=update_modifiedCount_zero, will stop task");
		} catch (Exception e) {
			if (isCloud) {
				logger.warn("TaskHA event=ping_failed_cloud_keep_running error={}", e.getMessage(), e);
				return;
			}
			long now = System.currentTimeMillis();
			long elapsedSinceLastSuccess = now - latestSuccessPingTime;
            long taskHeartTimeout = getTaskHeartTimeout();
			if (elapsedSinceLastSuccess <= taskHeartTimeout) {
				logger.warn("TaskHA event=ping_failed_retry elapsedSinceLastSuccessMs={} giveupAfterMs={}",
						elapsedSinceLastSuccess, taskHeartTimeout);
				return;
			}
			logger.warn("TaskHA event=ping_failed_giveup elapsedSinceLastSuccessMs={}, will stop task: {}",
					elapsedSinceLastSuccess, e.getMessage(), e);
		}

		internalStop();
    }

	private void internalStop() {
		taskMonitor.accept(TerminalMode.INTERNAL_STOP);
		stopTask.get();
		ExecutorUtil.shutdown(executorService, 1L, TimeUnit.SECONDS);
	}

	private long getTaskHeartTimeout() {
		try {
			SettingService settingService = BeanUtil.getBean(SettingService.class);
			if (settingService != null) {
				return settingService.getLong("jobHeartTimeout", DEFAULT_TASK_HEART_TIMEOUT_MS);
			}
		} catch (Exception e) {
			logger.warn("Get task heart timeout from setting failed, use default {}ms", DEFAULT_TASK_HEART_TIMEOUT_MS, e);
		}
		return DEFAULT_TASK_HEART_TIMEOUT_MS;
	}

	@Override
	public String describe() {
		return "Report task ping time monitor";
	}
}

package io.tapdata.flow.engine.V2.monitor.impl;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.AppType;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.ping.PingDto;
import com.tapdata.tm.commons.ping.PingType;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.util.SupplierImpl;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.websocket.ManagementWebsocketHandler;
import io.tapdata.websocket.WebSocketEvent;
import io.tapdata.websocket.handler.PongHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.web.socket.TextMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
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
	private final static long PING_INTERVAL_MS = 5000L;
	public static final String TAG = TaskPingTimeMonitor.class.getSimpleName();

	private Logger logger = LogManager.getLogger(TaskPingTimeMonitor.class);
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
					Thread.currentThread().setName("Task-PingTime-" + taskDto.getId().toHexString());

					Query query = new Query(where("_id").is(taskDto.getId())
							.and("status").nin(TaskDto.STATUS_ERROR, TaskDto.STATUS_SCHEDULE_FAILED)
							.and("agentId").is(taskDto.getAgentId())
					);
					Update update = new Update().set("pingTime", System.currentTimeMillis());
					try {
						Map<String, Object> pingData = new HashMap<String, Object>() {{
							put("where", query.getQueryObject().toJson());
							put("update", update.getUpdateObject().toJson());
						}};
						PingDto pingDto = new PingDto();
						pingDto.setPingType(PingType.TASK_PING);
						String pingId = UUIDGenerator.uuid();
						pingDto.setPingId(pingId);
						pingDto.setData(pingData);
						WebSocketEvent<PingDto> webSocketEvent = new WebSocketEvent<>();
						webSocketEvent.setType("ping");
						webSocketEvent.setData(pingDto);
						BeanUtil.getBean(ManagementWebsocketHandler.class).sendMessage(new TextMessage(JSONUtil.obj2Json(webSocketEvent)));
						boolean handleResponse = PongHandler.handleResponse(
								pingId,
								cache -> {
									String pingResult = cache.get(PingDto.PING_RESULT).toString();
									if (PingDto.PingResult.FAIL.name().equals(pingResult)) {
										throw new RuntimeException("Failed to send task heartbeat use websocket, will retry use http, message: " + cache.getOrDefault(PingDto.ERR_MESSAGE, "unknown error"));
									}
								}
						);
						if (!handleResponse) {
							throw new RuntimeException("No response from task heartbeat websocket, will retry use http");
						}
					} catch (Exception e) {
						logger.warn(e.getMessage(), e);
						taskPingTimeUseHttp(query, update);
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
		try {
			UpdateResult updateResult = clientMongoOperator.update(query, update, ConnectorConstant.TASK_COLLECTION);
			// 任务状态异常，应该将任务停止
			if (updateResult.getModifiedCount() == 0) {
				if (!AppType.init().isCloud()) {
					logger.warn("Send task ping time failed, will stop task");
					stopTask.get();
				}
			}
		} catch (Exception e) {
			if (!AppType.init().isCloud()) {
				logger.warn("Send task ping time failed, will stop task: {}", e.getMessage(), e);
				stopTask.get();
			}
		}
	}

	@Override
	public String describe() {
		return "Report task ping time monitor";
	}
}

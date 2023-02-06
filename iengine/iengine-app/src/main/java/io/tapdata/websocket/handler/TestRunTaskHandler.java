package io.tapdata.websocket.handler;

import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@EventHandlerAnnotation(type = "testRun")
public class TestRunTaskHandler implements WebSocketEventHandler<WebSocketEventResult> {

	private final static Logger logger = LogManager.getLogger(TestRunTaskHandler.class);

	private ClientMongoOperator clientMongoOperator;

	private TaskService<TaskDto> taskService;

	private final Map<String, TaskDto> taskDtoMap = new ConcurrentHashMap<>();

	private static final Map<String, TaskClient<TaskDto>> taskClientMap = new ConcurrentHashMap<>();


	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void initialize(TaskService<TaskDto> taskService, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.initialize(clientMongoOperator, settingService);
		this.taskService = taskService;
	}

	@Override
	public WebSocketEventResult handle(Map event) {

		long startTs = System.currentTimeMillis();
		TaskDto taskDto = JSONUtil.map2POJO(event, TaskDto.class);
		Log4jUtil.setThreadContext(taskDto);
		taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC);

		String taskId = taskDto.getId().toHexString();
		if (taskDtoMap.putIfAbsent(taskId, taskDto) != null) {
			logger.warn("{} task is running, skip", taskId);
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_RUN, "task is running...");
		}
		logger.info("{} task start", taskId);
		TaskClient<TaskDto> taskClient = null;
		try {
			taskClient = taskService.startTestTask(taskDto);
			taskClientMap.put(taskId, taskClient);
			taskClient.join();
			AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(taskClient.getError()));
		} catch (Throwable throwable) {
			logger.error(taskId + " task error", throwable);
			if (taskClient != null) {
				AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(throwable));
			}
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_RUN, throwable.getMessage());
		} finally {
			taskDtoMap.remove(taskId);
		}

		logger.info("test run task {} {}, cost {}ms", taskId, taskClient.getStatus(), (System.currentTimeMillis() - startTs));

		return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_RUN, true);
	}

	public static void setError(String taskId, Throwable error) {
		TaskClient<TaskDto> taskDtoTaskClient = taskClientMap.get(taskId);
		if (taskDtoTaskClient != null) {
			taskDtoTaskClient.error(error);
		}
	}

}

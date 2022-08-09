package io.tapdata.websocket.handler;

import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
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

	private TaskService<SubTaskDto> taskService;

	private Map<String, SubTaskDto> taskClientMap = new ConcurrentHashMap<>();


	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void initialize(TaskService<SubTaskDto> taskService, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.initialize(clientMongoOperator, settingService);
		this.taskService = taskService;
	}

	@Override
	public WebSocketEventResult handle(Map event) {

		long startTs = System.currentTimeMillis();
		SubTaskDto subTaskDto = JSONUtil.map2POJO(event, SubTaskDto.class);
		subTaskDto.getParentTask().setType(ParentTaskDto.TYPE_INITIAL_SYNC);

		String taskId = subTaskDto.getId().toHexString();
		if (taskClientMap.putIfAbsent(taskId, subTaskDto) != null) {
			logger.warn("{} task is running, skip", taskId);
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_RUN, "task is running...");
		}
		logger.info("{} task start", taskId);
		TaskClient<SubTaskDto> taskClient = null;
		try {
			taskClient = taskService.startTestTask(subTaskDto);
			taskClient.join();
			AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()));
		} catch (Throwable throwable) {
			logger.error(taskId + " task error", throwable);
			if (taskClient != null) {
				AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(throwable));
			}
			return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.TEST_RUN, throwable.getMessage());
		} finally {
			taskClientMap.remove(taskId);
		}

		logger.info("test run task {} {}, cost {}ms", taskId, taskClient.getStatus(), (System.currentTimeMillis() - startTs));

		return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_RUN, true);
	}

}

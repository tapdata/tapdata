package io.tapdata.websocket.handler;

import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.ClientMongoOperator;
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

@EventHandlerAnnotation(type = "testRun")
public class TestRunTaskHandler implements WebSocketEventHandler<WebSocketEventResult> {

	private final static Logger logger = LogManager.getLogger(TestRunTaskHandler.class);

	private ClientMongoOperator clientMongoOperator;

	private TaskService<SubTaskDto> taskService;

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

		String taskId = subTaskDto.getId().toHexString();
		TaskClient<SubTaskDto> taskClient = taskService.startTestTask(subTaskDto);
		try {
			taskClient.join();
		} finally {
			AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()));
		}

		logger.info("test run task {} {}, cost {}ms", taskId, taskClient.getStatus(), (System.currentTimeMillis() - startTs));

		return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.TEST_RUN, true);
	}

}

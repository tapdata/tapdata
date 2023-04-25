package io.tapdata.websocket.handler;

import com.alibaba.fastjson.JSON;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.entity.CheckAgainProgress;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.CheckAgainRunner;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventHandler;
import io.tapdata.websocket.WebSocketEventResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@EventHandlerAnnotation(type = "autoInspectAgain")
public class AutoInspectAgainHandler implements WebSocketEventHandler<WebSocketEventResult> {

	private final static Logger logger = LogManager.getLogger(AutoInspectAgainHandler.class);
	private static final ExecutorService EXECUTORS = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());

	private ClientMongoOperator clientMongoOperator;
	private SettingService settingService;

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	@Override
	public void initialize(TaskService<TaskDto> taskService, ClientMongoOperator clientMongoOperator, SettingService settingService) {
		this.initialize(clientMongoOperator, settingService);
		this.settingService = settingService;
	}

	@Override
	public WebSocketEventResult handle(Map event) {
		String taskId = (String) event.get("taskId");
		CheckAgainProgress progress = JSON.parseObject((String) event.get("data"), CheckAgainProgress.class);

		EXECUTORS.submit(new CheckAgainRunner(taskId, progress, clientMongoOperator, settingService));

		return WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.AUTO_INSPECT_AGAIN, true);
	}
}

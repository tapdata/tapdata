package io.tapdata.websocket.handler;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.flow.engine.V2.task.cleaner.OpType;
import io.tapdata.flow.engine.V2.task.cleaner.TaskCleanerContext;
import io.tapdata.flow.engine.V2.task.cleaner.TaskCleanerService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventResult;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-23 12:05
 **/
@EventHandlerAnnotation(type = "dataSync")
public class DataSyncEventHandler extends BaseEventHandler {
	private static final String TAG = DataSyncEventHandler.class.getSimpleName();

	@Override
	public void initialize(ClientMongoOperator clientMongoOperator) {
		super.initialize(clientMongoOperator);
	}

	@Override
	public Object handle(Map event) {
		WebSocketEventResult webSocketEventResult = WebSocketEventResult.handleSuccess(WebSocketEventResult.Type.DATA_SYNC_RESULT, event);
		try {
			String opTypeStr = (String) event.getOrDefault("opType", "");
			String taskId = (String) event.getOrDefault("taskId", "");
			OpType opType = OpType.fromOp(opTypeStr);
			if (null != opType) {
				switch (opType) {
					case RESET:
					case DELETE:
						TaskCleanerService.clean(new TaskCleanerContext(taskId, clientMongoOperator), opTypeStr);
						break;
					default:
						break;
				}
			}
			/*TaskCleaner taskCleaner = TaskCleaner.create(opType, taskId, clientMongoOperator);
			taskCleaner.clean();
			if (StringUtils.isBlank(opType)) {
				return WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, "Op type is empty");
			}
			switch (opType) {
				case "reset":
				case "delete":
					if (StringUtils.isBlank(taskId)) {
						webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, "Task id is empty");
						break;
					}
					try {
						destroy(taskId);
					} catch (Throwable e) {
						if ("delete".equals(opType)) {
							logger.error("Destroy task [" + taskId + "] failed, error: " + e.getMessage(), e);
						} else {
							webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
						}
					}
					break;
				default:
					break;
			}*/
		} catch (Throwable e) {
			webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
		}
		return webSocketEventResult;

	}
}

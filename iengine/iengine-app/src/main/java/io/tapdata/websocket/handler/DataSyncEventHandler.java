package io.tapdata.websocket.handler;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.OpType;
import io.tapdata.flow.engine.V2.task.cleaner.TaskCleanerContext;
import io.tapdata.flow.engine.V2.task.cleaner.TaskCleanerService;
import io.tapdata.websocket.EventHandlerAnnotation;
import io.tapdata.websocket.WebSocketEventResult;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

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
			TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
			if (null != opType) {
				switch (opType) {
					case START:
						Query query = Query.query(where("id").is(taskId));
						Update update = Update.update(TaskDto.PING_TIME_FIELD, System.currentTimeMillis());
						TaskDto taskDto = clientMongoOperator.findAndModify(query, update, TaskDto.class, ConnectorConstant.TASK_COLLECTION, true);
						logger.info("Start task from websocket event: {}", event);
						tapdataTaskScheduler.startTask(taskDto);
						break;
					case STOP:
						logger.info("Stop task from websocket event: {}", event);
						tapdataTaskScheduler.stopTask(taskId);
						break;
					case RESET:
					case DELETE:
						TaskCleanerService.clean(new TaskCleanerContext(taskId, clientMongoOperator), opTypeStr);
						break;
					default:
						logger.warn("Unrecognized data sync event op type: {}, event: {}", opTypeStr, event);
						break;
				}
			} else {
				logger.warn("Unrecognized data sync event op type: {}, event: {}", opTypeStr, event);
			}
		} catch (Throwable e) {
			webSocketEventResult = WebSocketEventResult.handleFailed(WebSocketEventResult.Type.DATA_SYNC_RESULT, e.getMessage() + "\n" + Log4jUtil.getStackString(e), e);
		}
		return webSocketEventResult;

	}
}

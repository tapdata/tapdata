package com.tapdata.processor.dataflow;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.error.RowFilterProcessorExCode_24;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.exception.TapCodeException;
import io.tapdata.utils.ErrorCodeUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.script.Invocable;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jackin
 */
public class RowFilterProcessor implements DataFlowProcessor {

	private static final String SCRIPT_FUNCTION_NAME = "filter";

	private static final String ROW_FILTER_SCRIPT_TEMPLATE = "function %s(record){\n" +
			"return !!(%s)\n" +
			"}\n";
	private Logger logger = LogManager.getLogger(getClass());

	private ProcessorContext context;

	private Stage stage;

	private Invocable engine;

	private FilterAction action;

	private Map<String, Object> processContext;
	private boolean withoutBeforeLog;

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.context = context;
		this.stage = stage;

		action = FilterAction.fromString(stage.getAction());
		if (action == null) {
			throw new RuntimeException(String.format("Unknown row filter action %s", action));
		}

		String expression = stage.getExpression();
		if (StringUtils.isBlank(expression)) {
			throw new RuntimeException(String.format("Row filter expression cannot be empty"));
		}

		String script = String.format(ROW_FILTER_SCRIPT_TEMPLATE, SCRIPT_FUNCTION_NAME, expression);

		ScriptConnection sourceScriptConnection = context.getSourceScriptConnection();
		ScriptConnection targetScriptConnection = context.getTargetScriptConnection();

		try {
			this.engine = ScriptUtil.getScriptEngine(stage.getJsEngineName(), script, context.getJavaScriptFunctions(), context.getClientMongoOperator(), sourceScriptConnection, targetScriptConnection, null, logger);
		} catch (ScriptException e) {
			throw new RuntimeException(String.format("Initial row filter expression %s failed, format script %s", expression, script));
		}

		processContext = new ConcurrentHashMap<>();
		withoutBeforeLog = false;
	}

	private TapLogger.LogListener logListener;
	public void logListener(TapLogger.LogListener logListener){
		this.logListener = logListener;
	}

	public MessageEntity process(MessageEntity message) {

		if (message == null) {
			return message;
		}
		String messageOp = message.getOp();
		if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT.equals(messageOp)
		) {
			Map<String, Object> record = MapUtils.isNotEmpty(message.getAfter()) ? message.getAfter() : message.getBefore();
			try {
				if (MapUtils.isNotEmpty(record)) {
					if ("u".equals(message.getOp())) {
						if (MapUtils.isEmpty(message.getBefore()) && !withoutBeforeLog && null != logListener) {
							logListener.warn("current database does not support record before value when update, the data will insert or delete record if after value meets condition");
							withoutBeforeLog = true;
						}
						Object before = ScriptUtil.invokeScript(engine, SCRIPT_FUNCTION_NAME, message, context.getSourceConn(), context.getTargetConn(), context.getJob(), processContext, logger, "before");
						Object after = ScriptUtil.invokeScript(engine, SCRIPT_FUNCTION_NAME, message, context.getSourceConn(), context.getTargetConn(), context.getJob(), processContext, logger, "after");
						if (isTrue(before) && !isTrue(after)) { // before满足，after不满足
							beforeMeetMessage(message);
						} else if (!isTrue(before) && isTrue(after)) { // before不满足，after满足
							afterMeetMessage(message);
						} else if (isTrue(before) && isTrue(after)) {
							if (MapUtils.isEmpty(message.getBefore())) {
								afterMeetMessage(message);
							} else {
								message = FilterAction.DISCARD == action ? null : message;
							}
						} else { // 不满足
							if (MapUtils.isEmpty(message.getBefore())) {
								beforeMeetMessage(message);
							}else {
								message = FilterAction.DISCARD == action ? message : null;
							}
						}
					} else {
						Object o = ScriptUtil.invokeScript(engine, SCRIPT_FUNCTION_NAME, message, context.getSourceConn(), context.getTargetConn(), context.getJob(), processContext, logger, null);
						// 满足条件处理
						if (isTrue(o)) {
							message = FilterAction.DISCARD == action ? null : message;
						}
						// 不满足条件处理
						else {
							message = FilterAction.DISCARD == action ? message : null;
						}
					}
				}
			} catch (Exception e) {
				TapCodeException tapCodeException = new TapCodeException(RowFilterProcessorExCode_24.JAVA_SCRIPT_ERROR, e)
						.dynamicDescriptionParameters(ErrorCodeUtils.truncateData(record));
				context.getJob().jobError(tapCodeException, false, OffsetUtil.getSyncStage(message.getOffset()), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
						TapLog.PROCESSOR_ERROR_0005.getMsg(), null, record, e.getMessage());
			}
		}
		return message;
	}

	private void beforeMeetMessage(MessageEntity message) {
		if (FilterAction.DISCARD == action) {
			message.setOp("i");
			message.setBefore(null);
		} else {
			message.setOp("d");
			if (MapUtils.isEmpty(message.getBefore())) {
				message.setBefore(message.getAfter());
			}
			message.setAfter(null);
		}
	}

	private void afterMeetMessage(MessageEntity message) {
		if (FilterAction.DISCARD == action) {
			message.setOp("d");
			if (MapUtils.isEmpty(message.getBefore())) {
				message.setBefore(message.getAfter());
			}
			message.setAfter(null);
		} else {
			message.setOp("i");
			message.setBefore(null);
		}
	}

	private boolean isTrue(Object o) {
		if (o instanceof Boolean) {
			return (Boolean) o;
		}
		return false;
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (CollectionUtils.isNotEmpty(batch)) {
			for (int i = 0; i < batch.size(); i++) {
				MessageEntity messageEntity = batch.get(i);
				MessageEntity processResult = process(messageEntity);
				if (processResult == null) {
					messageEntity.setOp(OperationType.COMMIT_OFFSET.getOp());
					messageEntity.setAfter(null);
					messageEntity.setBefore(null);
				}
			}
		}
		return batch;
	}

	@Override
	public void stop() {

	}

	@Override
	public Stage getStage() {
		return stage;
	}


	enum FilterAction {
		DISCARD("discard"),
		RETAIN("retain"),
		;

		private String action;

		FilterAction(String action) {
			this.action = action;
		}

		public static FilterAction fromString(String action) {
			for (FilterAction filterAction : FilterAction.values()) {
				if (filterAction.action.equals(action)) {
					return filterAction;
				}
			}

			return null;
		}
	}

}

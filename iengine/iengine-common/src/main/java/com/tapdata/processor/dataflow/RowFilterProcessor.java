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
import io.tapdata.exception.TapCodeException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
					Object o = ScriptUtil.invokeScript(engine, SCRIPT_FUNCTION_NAME, message, context.getSourceConn(), context.getTargetConn(), context.getJob(), processContext, logger);
					if (o instanceof Boolean) {
						Boolean result = (Boolean) o;
						// 满足条件处理
						if (result != null && result) {
							message = FilterAction.DISCARD == action ? null : message;
						}
						// 不满足条件处理
						else {
							message = FilterAction.DISCARD == action ? message : null;
						}
					}
					// 不满足条件处理
					else {
						message = FilterAction.DISCARD == action ? message : null;
					}
				}
			} catch (Exception e) {
				TapCodeException tapCodeException = new TapCodeException(RowFilterProcessorExCode_24.JAVA_SCRIPT_ERROR, e);
				context.getJob().jobError(tapCodeException, false, OffsetUtil.getSyncStage(message.getOffset()), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
						TapLog.PROCESSOR_ERROR_0005.getMsg(), null, record, e.getMessage());
			}
		}
		return message;
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

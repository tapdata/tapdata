package com.tapdata.processor.dataflow;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JdbcUtil;
import com.tapdata.constant.MapUtilV2;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.constant.TapList;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.FieldProcess;
import com.tapdata.entity.FieldScript;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.dataflow.CloneFieldProcess;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.FieldProcessUtil;
import com.tapdata.processor.ProcessorUtil;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.indices.IndicesUtil;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.comment.Comment;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeanUtils;

import javax.script.Invocable;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author jackin
 */
public class FieldDataFlowProcessor implements DataFlowProcessor {

	private Logger logger = LogManager.getLogger(getClass());

	private Stage stage;

	private ProcessorContext context;

	private List<FieldProcess> fieldProcesses;

	private List<FieldScript> fieldScripts;

	private final Map<String, Map<String, Invocable>> fieldScriptEngine;

	private final static String SCRIPT_TEMPLATE = "function process(record){ return %s;}";

	private Map<String, Object> processContext;

	private List<CloneFieldProcess> cloneFieldProcesses;

	private HashSet<String> tableNames;

	private DatabaseTypeEnum targetDatabaseTypeEnum;

	private boolean deleteAllFields;
	private Set<String> rollbackRemoveFields;

	public FieldDataFlowProcessor() {
		this.fieldScriptEngine = new ConcurrentHashMap<>();
	}

	public FieldDataFlowProcessor(boolean deleteAllFields) {
		this.deleteAllFields = deleteAllFields;
		this.fieldScriptEngine = new ConcurrentHashMap<>();
	}

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.stage = stage;
		this.context = context;

		this.fieldProcesses = stage.getOperations();
		this.cloneFieldProcesses = stage.getField_process();
		if (CollectionUtils.isNotEmpty(fieldProcesses)) {
			FieldProcessUtil.sortFieldProcess(fieldProcesses);
		}
		this.fieldScripts = stage.getScripts();
		Connections targetConn = context.getTargetConn();
		if (targetConn != null) {
			this.targetDatabaseTypeEnum = DatabaseTypeEnum.fromString(targetConn.getDatabase_type());
		}


		processContext = new ConcurrentHashMap<>();
		tableNames = new HashSet<>();
		this.rollbackRemoveFields = CollectionUtils.isEmpty(fieldProcesses) ? new HashSet<>() : fieldProcesses.stream()
				.filter(f -> FieldProcess.FieldOp.OP_REMOVE.equals(FieldProcess.FieldOp.fromOperation(f.getOp())) && f.getOperand().equals("false"))
				.map(FieldProcess::getField).collect(Collectors.toSet());
	}

	protected Invocable getOrInitEngine(String fieldName, String script) {
		return fieldScriptEngine.computeIfAbsent(Thread.currentThread().getName(), tn -> new HashMap<>())
				.computeIfAbsent(fieldName, fn -> {
					try {
						ScriptConnection sourceScriptConnection = context.getSourceScriptConnection();
						ScriptConnection targetScriptConnection = context.getTargetScriptConnection();
						return ScriptUtil.getScriptEngine(
								JSEngineEnum.GRAALVM_JS.getEngineName(),
								String.format(SCRIPT_TEMPLATE, script),
								context.getJavaScriptFunctions(),
								context.getClientMongoOperator(),
								sourceScriptConnection,
								targetScriptConnection,
								null,
								logger);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
	}

	@Override
	public Stage getStage() {
		return stage;
	}

	public MessageEntity process(MessageEntity message) throws Exception {
		if (message == null) {
			return message;
		}
		String messageOp = message.getOp();
		if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp)
		) {
			Map<String, Object> record = MapUtils.isNotEmpty(message.getAfter()) ? message.getAfter() : message.getBefore();

			if (record == null) {
				record = new HashMap<>();
			}
			if (fieldProcesses != null) {
				// field process may cause field name change, so we should call for both after and before
				// so that it cat be get by mapping
				Map<String, Object> before = message.getBefore();
				if (MapUtils.isNotEmpty(before)) {
					FieldProcessUtil.filedProcess(before, fieldProcesses, rollbackRemoveFields, deleteAllFields);
					message.setBefore(before);
				}
				Map<String, Object> after = message.getAfter();
				if (MapUtils.isNotEmpty(after)) {
					FieldProcessUtil.filedProcess(after, fieldProcesses, rollbackRemoveFields, deleteAllFields);
					message.setAfter(after);
				}
				if (null != before){
					fieldScript(message, before, "before");
				}
				if (null != after){
					fieldScript(message, after, "after");
				}
			} else if (CollectionUtils.isNotEmpty(cloneFieldProcesses)) {
				// cluster clone field process
				CloneFieldProcess cloneFieldProcess = cloneFieldProcesses.parallelStream()
						.filter(cp -> StringUtils.isNotBlank(cp.getTable_name()) && cp.getTable_name().equals(message.getTableName()))
						.findFirst().orElse(null);
				if (cloneFieldProcess == null) {
					return message;
				}

				FieldProcessUtil.filedProcess(record, cloneFieldProcess.getOperations());
			} else {
				return message;
			}

			if (MapUtils.isNotEmpty(record)) {

				if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageOp) ||
						ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(messageOp) ||
						ConnectorConstant.MESSAGE_OPERATION_ABSOLUTE_INSERT.equals(messageOp)) {
					message.setAfter(record);
				} else {
					message.setBefore(record);
				}
			}
		} else if (OperationType.CREATE_INDEX.getOp().equalsIgnoreCase(messageOp)) {
			List<FieldProcess> tmpFieldProcess = getFieldProcesses(message);
			// 判断是否丢弃事件
			if (null != tmpFieldProcess && !FieldProcessUtil.filedProcess(IndicesUtil.getTableIndex(message), tmpFieldProcess)) {
				return null;
			}
		} else if (OperationType.DDL.getOp().equalsIgnoreCase(messageOp)) {
			//ddl事件，处理字段信息
			List<FieldProcess> tmpFieldProcess = getFieldProcesses(message);
			if (CollectionUtils.isNotEmpty(tmpFieldProcess)) {
				Map<String, FieldProcess> fieldProcessMap = tmpFieldProcess.stream()
						.collect(Collectors.toMap(f -> JdbcUtil.formatFieldName(f.getField(), targetDatabaseTypeEnum.getType()), Function.identity()));

				String originalDdl = message.getDdl();
				Statement parse = CCJSqlParserUtil.parse(originalDdl);
				boolean isContinue = false;
				if (parse instanceof Alter) {
					isContinue = FieldProcessUtil.alterProcess((Alter) parse, fieldProcessMap, targetDatabaseTypeEnum);
				} else if (parse instanceof Comment) {
					isContinue = FieldProcessUtil.commentProcess((Comment) parse, fieldProcessMap, targetDatabaseTypeEnum);
				}
				String ddl = parse.toString();
				if (!isContinue) {
					logger.warn("field process ddl {} to {}, ignore..", originalDdl, ddl);
					return null;
				}
				logger.info("field process ddl {} to {}", originalDdl, ddl);
				message.setDdl(ddl);
			}
		}

		return message;
	}

	@Nullable
	private List<FieldProcess> getFieldProcesses(MessageEntity message) {
		List<FieldProcess> tmpFieldProcess = null;
		if (fieldProcesses != null) {
			// custom field process
			tmpFieldProcess = fieldProcesses;
		} else if (CollectionUtils.isNotEmpty(cloneFieldProcesses)) {
			// cluster clone field process
			CloneFieldProcess cloneFieldProcess = cloneFieldProcesses.parallelStream()
					.filter(cp -> StringUtils.isNotBlank(cp.getTable_name()) && cp.getTable_name().equals(message.getTableName()))
					.findFirst().orElse(null);
			if (null != cloneFieldProcess) {
				tmpFieldProcess = cloneFieldProcess.getOperations();
			}
		}
		return tmpFieldProcess;
	}


	protected void fieldScript(MessageEntity message, Map<String, Object> originRecord, String tag) {
		if (CollectionUtils.isNotEmpty(fieldScripts)) {
			for (FieldScript fieldScript : fieldScripts) {
				String fieldName = fieldScript.getField();
				String script = fieldScript.getScript();
				Invocable engine = getOrInitEngine(fieldName, script);
				Object valueByKey = MapUtilV2.getValueByKeyV2(originRecord, fieldName);
				try {
					if (valueByKey instanceof TapList) {
						MessageEntity finalMessage = new MessageEntity();
						BeanUtils.copyProperties(message, finalMessage);
						finalMessage.setAfter(originRecord);
						CollectionUtil.tapListValueInvokeFunction((TapList) valueByKey,
								o -> {
									try {
										return ScriptUtil.invokeScript(engine, ScriptUtil.FUNCTION_NAME, finalMessage, context.getSourceConn(),
												context.getTargetConn(), context.getJob(), processContext, logger, tag);
									} catch (Exception e) {
										throw new RuntimeException(e);
									}
								});
						MapUtilV2.putValueInMap(originRecord, fieldName, valueByKey);
					} else {
						Object o = ScriptUtil.invokeScript(engine, ScriptUtil.FUNCTION_NAME, message, context.getSourceConn(),
								context.getTargetConn(), context.getJob(), processContext, logger, tag);
						MapUtilV2.putValueInMap(originRecord, fieldName, o);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		for (MessageEntity messageEntity : batch) {
			try {
				tableNames.add(messageEntity.getTableName());
				process(messageEntity);
			} catch (Exception e) {
				if (context.getJob().jobError(e, false, OffsetUtil.getSyncStage(messageEntity), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
						e.getMessage(), null)) {
					continue;
				} else {
					break;
				}
			}
		}
		return batch;
	}

	@Override
	public void stop() {
		ProcessorUtil.closeScriptConnection(context);
	}
}

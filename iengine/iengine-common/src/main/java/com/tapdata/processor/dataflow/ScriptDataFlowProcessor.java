package com.tapdata.processor.dataflow;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MessageUtil;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.ProcessorUtil;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.script.Invocable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author jackin
 */
public class ScriptDataFlowProcessor implements DataFlowProcessor {

	private Logger logger = LogManager.getLogger(ScriptDataFlowProcessor.class);

	private ProcessorContext context;

	private Stage stage;

	private Invocable engine;

	private Map<String, Object> processContext;

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.context = context;
		this.stage = stage;

		String script = stage.getScript();

		ScriptConnection sourceScriptConnection = context.getSourceScriptConnection();
		ScriptConnection targetScriptConnection = context.getTargetScriptConnection();
		ICacheService cacheService = null;
		if (context.getCacheService() != null) {
			cacheService = context.getCacheService();
		}

		this.engine = ScriptUtil.getScriptEngine(
				stage.getJsEngineName(),
				script,
				context.getJavaScriptFunctions(),
				context.getClientMongoOperator(),
				sourceScriptConnection,
				targetScriptConnection,
				cacheService,
				logger);

		processContext = new ConcurrentHashMap<>();

	}

	public List<MessageEntity> process(MessageEntity message) {

		if (message == null) {
			return null;
		}
		List<MessageEntity> resultList = new ArrayList<>();
		String messageOp = message.getOp();
		if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp)
		) {
			Map<String, Object> record = MapUtils.isNotEmpty(message.getAfter()) ? message.getAfter() : message.getBefore();
			try {
				if (MapUtils.isNotEmpty(record)) {

					Object o = ScriptUtil.invokeScript(engine, ScriptUtil.FUNCTION_NAME, message, context.getSourceConn(), context.getTargetConn(), context.getJob(), processContext, logger);

					if (o == null) {
						return null;
					} else if (o instanceof List) {
						for (Object obj : (List) o) {
							final MessageEntity newMsg = (MessageEntity) message.clone();
							convertMessage(newMsg, messageOp, record, (Map<String, Object>) obj);
							resultList.add(newMsg);
						}
					} else {
						convertMessage(message, messageOp, record, (Map<String, Object>) o);
						resultList.add(message);
					}
				}
			} catch (Exception e) {
				context.getJob().jobError(e, true, OffsetUtil.getSyncStage(message.getOffset()), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
						TapLog.PROCESSOR_ERROR_0005.getMsg(), null, record, e.getMessage());
			}
		} else {
			resultList.add(message);
		}
		return resultList;
	}

	private void convertMessage(MessageEntity message, String messageOp, Map<String, Object> record, Map<String, Object> o) {
		Map<String, Object> newMap = new HashMap<>();
		MapUtil.copyToNewMap(o, newMap);
		record.clear();

		if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp)) {
			message.setBefore(newMap);
		} else {
			message.setAfter(newMap);
		}

		if (processContext.get("op") != null && !StringUtils.equals(message.getOp(), processContext.get("op").toString())) {
			MessageUtil.convertMessageOp(message, processContext.get("op").toString());
		}
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (CollectionUtils.isNotEmpty(batch)) {
			List<MessageEntity> resultBatch = new ArrayList<>();
			for (int i = 0; i < batch.size(); i++) {
				MessageEntity messageEntity = batch.get(i);
				List<MessageEntity> processResultList = process(messageEntity);
				if (CollectionUtils.isEmpty(processResultList)) {
					final Object offset = messageEntity.getOffset();
					if (offset instanceof TapdataOffset) {
						TapdataOffset tapdataOffset = (TapdataOffset) offset;
						if (TapdataOffset.SYNC_STAGE_CDC.equals(tapdataOffset.getSyncStage())) {
							messageEntity.setOp(OperationType.COMMIT_OFFSET.getOp());
							messageEntity.setAfter(null);
							messageEntity.setBefore(null);
							resultBatch.add(messageEntity);
						}
					}
				} else {
					resultBatch.addAll(processResultList);
				}
				if (!ConnectorConstant.RUNNING.equals(context.getJob().getStatus())) {
					break;
				}
			}
			batch = resultBatch;

//      for (int i = 0; i < batch.size(); i++) {
//        MessageEntity messageEntity = batch.get(i);
//        MessageEntity processResult = process(messageEntity);
//        if (processResult == null) {
//          final Object offset = messageEntity.getOffset();
//          if (offset != null && offset instanceof TapdataOffset) {
//            TapdataOffset tapdataOffset = (TapdataOffset) offset;
//            if (TapdataOffset.SYNC_STAGE_CDC.equals(tapdataOffset.getSyncStage())) {
//              messageEntity.setOp(OperationType.COMMIT_OFFSET.getOp());
//              messageEntity.setAfter(null);
//              messageEntity.setBefore(null);
//            } else {
//              batch.remove(i);
//              i--;
//            }
//          } else {
//            batch.remove(i);
//            i--;
//          }
//        }
//
//        if (!ConnectorConstant.RUNNING.equals(context.getJob().getStatus())) {
//          break;
//        }
//      }
		}
		return batch;
	}

	@Override
	public void stop() {
		ProcessorUtil.closeScriptConnection(context);
	}

	@Override
	public Stage getStage() {
		return stage;
	}

	public static void main(String[] args) {

	}
}

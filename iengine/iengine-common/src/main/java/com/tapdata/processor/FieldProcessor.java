package com.tapdata.processor;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.entity.FieldProcess;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.dataflow.Stage;
import io.tapdata.indices.IndicesUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.tapdata.entity.FieldProcess.FieldOp.OP_CONVERT;
import static com.tapdata.entity.FieldProcess.FieldOp.OP_REMOVE;

/**
 * @author jackin
 */
public class FieldProcessor implements Processor {

	private Logger logger = LogManager.getLogger(getClass());

	private Job job;

	//    private static Map<String, Set<String>> dataTypeConvertMapping = new HashMap<>();
//
	private static Set<String> SUPPORTED_DATA_TYPE = new HashSet<>();

	private Map<String, Stage> stageMap;

	static {
		// number type
		SUPPORTED_DATA_TYPE.add("INT");
		SUPPORTED_DATA_TYPE.add("INTEGER");
		SUPPORTED_DATA_TYPE.add("DOUBLE");
		SUPPORTED_DATA_TYPE.add("SHORT");
		SUPPORTED_DATA_TYPE.add("FLOAT");
		SUPPORTED_DATA_TYPE.add("LONG");
		SUPPORTED_DATA_TYPE.add("STRING");
		SUPPORTED_DATA_TYPE.add("DATE");
		SUPPORTED_DATA_TYPE.add("BOOLEAN");
	}

	public FieldProcessor(Job job) throws Exception {
		this.job = job;

		List<Mapping> mappings = job.getMappings();

		AtomicReference<String> field = new AtomicReference<>();
		if (!validate(mappings, (errorField) -> {
			field.set(errorField);
		})) {
			logger.error(TapLog.PROCESSOR_ERROR_0001.getMsg(), field.get());

			throw new Exception();
		}

		for (Mapping mapping : mappings) {

			List<FieldProcess> fieldsProcess = mapping.getFields_process();
			if (CollectionUtils.isNotEmpty(fieldsProcess)) {
				Collections.sort(fieldsProcess);
			}

			stageMap = new HashMap<>();
			List<Stage> stages = mapping.getStages();
			if (CollectionUtils.isNotEmpty(stages)) {
				for (Stage stage : stages) {
					Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(stage.getType());
					if (Stage.StageTypeEnum.FIELD_PROCESSOR == stageTypeEnum) {
						stageMap.put(mapping.getFrom_table(), stage);
					}
				}
			}

		}
	}

	public MessageEntity process(MessageEntity message) throws Exception {

		if (message == null) {
			return message;
		}
		String messageOp = message.getOp();
		if (ConnectorConstant.MESSAGE_OPERATION_INSERT.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(messageOp) ||
				ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(messageOp)
		) {
			Map<String, Object> record = MapUtils.isNotEmpty(message.getAfter()) ? message.getAfter() : message.getBefore();
			String actualTableName = message.getTableName();
			List<Mapping> mappings = job.getMappings();
			if (CollectionUtils.isNotEmpty(mappings)) {
				for (Mapping mapping : mappings) {
					String expectTable = mapping.getFrom_table();
					if (!expectTable.equals(actualTableName)) {
						continue;
					}

					List<FieldProcess> filedsProcesses = mapping.getFields_process();
					if (CollectionUtils.isEmpty(filedsProcesses)) {
						continue;
					}

					FieldProcessUtil.filedProcess(record, filedsProcesses);

				}
			}
		} else if (OperationType.CREATE_INDEX.getOp().equalsIgnoreCase(messageOp)) {
			message = (MessageEntity) message.clone();
			String actualTableName = message.getTableName();
			List<Mapping> mappings = job.getMappings();
			if (CollectionUtils.isNotEmpty(mappings)) {
				for (Mapping mapping : mappings) {
					String expectTable = mapping.getFrom_table();
					if (!expectTable.equals(actualTableName)) continue;

					List<FieldProcess> filedsProcesses = mapping.getFields_process();
					if (CollectionUtils.isEmpty(filedsProcesses)) continue;

					// 判断是否丢弃事件
					if (!FieldProcessUtil.filedProcess(IndicesUtil.getTableIndex(message), filedsProcesses)) {
						return null;
					}
					break;
				}
			}
		}

		return message;
	}

	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {

		for (MessageEntity messageEntity : batch) {
			try {
				process(messageEntity);
			} catch (Exception e) {
				if (!job.jobError(e, false, OffsetUtil.getSyncStage(messageEntity), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
						e.getMessage(), null)) {
					break;
				}
			}
		}
		return batch;
	}

	@Override
	public void stop() {

	}

	private boolean validate(List<Mapping> mappings, Consumer<String> errorField) {
		for (Mapping mapping : mappings) {
			List<FieldProcess> fieldsProcess = mapping.getFields_process();

			if (CollectionUtils.isNotEmpty(fieldsProcess)) {
				Set<String> keySet = mappingKeySet(mapping);
				for (FieldProcess process : fieldsProcess) {
					String field = process.getField();
					String op = process.getOp();

					if (FieldProcess.FieldOp.fromOperation(op) == OP_REMOVE && keySet.contains(field)) {

						errorField.accept(field);
						return false;
					}

					String operand = process.getOperand();
					String originedatatype = process.getOriginedatatype();
					if (FieldProcess.FieldOp.fromOperation(op) == OP_CONVERT && !SUPPORTED_DATA_TYPE.contains(operand.toUpperCase())) {
						logger.warn(TapLog.PROCESSOR_ERROR_0002.getMsg(), originedatatype, operand);
					}

				}
			}

		}

		return true;
	}

	public Map<String, Stage> getStageMap() {
		return stageMap;
	}

	private Set<String> mappingKeySet(Mapping mapping) {
		List<Map<String, String>> joinCondition = mapping.getJoin_condition();
		List<Map<String, String>> matchCondition = mapping.getMatch_condition();

		Set<String> keySet = new HashSet<>();
		if (CollectionUtils.isNotEmpty(joinCondition)) {
			for (Map<String, String> map : joinCondition) {
				keySet.add(map.get("source"));
			}
		}

		if (CollectionUtils.isNotEmpty(matchCondition)) {
			for (Map<String, String> map : matchCondition) {
				keySet.add(map.get("source"));
			}
		}

		return keySet;
	}
}

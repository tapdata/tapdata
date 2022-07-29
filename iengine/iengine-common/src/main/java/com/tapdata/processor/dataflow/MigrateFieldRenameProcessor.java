package com.tapdata.processor.dataflow;

import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MigrateFieldRenameProcessor implements DataFlowProcessor{

	private static Logger logger = LogManager.getLogger(MigrateFieldRenameProcessor.class);

	private ProcessorContext processorContext;

	private Stage stage;

	/**
	 * key: table name
	 *  --key old field name
	 */
	private final Map<String, Map<String, FieldInfo>> tableFieldsMappingMap;


	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (CollectionUtils.isEmpty(batch)) {
			return batch;
		}

		for (MessageEntity messageEntity : batch) {
			messageEntity.setBefore(processFields(messageEntity.getBefore(), messageEntity.getTableName()));
			messageEntity.setAfter(processFields(messageEntity.getAfter(), messageEntity.getTableName()));

		}
		return batch;
	}

	private Map<String, Object> processFields(Map<String, Object> map, String tableName) {
		if (MapUtils.isEmpty(map)) {
			return map;
		}
		Map<String, FieldInfo> fieldsMappingMap = this.tableFieldsMappingMap.get(tableName);
		if (MapUtils.isEmpty(fieldsMappingMap)) {
			return map;
		}

		Set<String> keySet = new HashSet<>(map.keySet());
		for (String oldFieldName : keySet) {
			FieldInfo fieldInfo = fieldsMappingMap.get(oldFieldName);
			if (fieldInfo == null) {
				logger.warn(String.format("no suitable rename configuration for field [%s]", oldFieldName));
				continue;
			}
			Object value = map.remove(oldFieldName);
			if (fieldInfo.getIsShow() != null && !fieldInfo.getIsShow()) {
				continue;
			}
			map.put(fieldInfo.getTargetFieldName(), value);
		}
		return map;
	}

	public MigrateFieldRenameProcessor(MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode) {
		this.tableFieldsMappingMap = migrateFieldRenameProcessorNode.getFieldsMapping().stream()
			.collect(Collectors.toMap(TableFieldInfo::getPreviousTableName,
				t->t.getFields().stream()
								.collect(Collectors.toMap(FieldInfo::getSourceFieldName, Function.identity()))));
	}

	@Override
	public void stop() {

	}

	@Override
	public void initialize(ProcessorContext context, Stage stage) throws Exception {
		this.processorContext = context;
		this.stage = stage;
	}

	@Override
	public Stage getStage() {
		return this.stage;
	}

}

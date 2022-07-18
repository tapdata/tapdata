package com.tapdata.processor.dataflow;

import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MigrateFieldRenameProcessor implements DataFlowProcessor {

	private ProcessorContext processorContext;

	private Stage stage;

	/**
	 * key: table name
	 * --key old field name
	 */
	private final Map<String, Map<String, String>> tableFieldsMappingMap;


	@Override
	public List<MessageEntity> process(List<MessageEntity> batch) {
		if (CollectionUtils.isEmpty(batch)) {
			return batch;
		}

		for (MessageEntity messageEntity : batch) {
			messageEntity.setBefore(renameFields(messageEntity.getBefore(), messageEntity.getTableName()));
			messageEntity.setAfter(renameFields(messageEntity.getAfter(), messageEntity.getTableName()));

		}
		return batch;
	}

	private Map<String, Object> renameFields(Map<String, Object> map, String tableName) {
		if (MapUtils.isEmpty(map)) {
			return map;
		}
		Map<String, String> fieldsMappingMap = this.tableFieldsMappingMap.get(tableName);
		if (MapUtils.isEmpty(fieldsMappingMap)) {
			throw new IllegalArgumentException(String.format("no suitable rename configuration for table [%s]", tableName));
		}

		Map<String, Object> copyMap = Collections.unmodifiableMap(new HashMap<>(map));
		for (String oldFieldName : copyMap.keySet()) {
			String newFieldName = fieldsMappingMap.get(oldFieldName);
			if (StringUtils.isEmpty(newFieldName)) {
				throw new IllegalArgumentException(String.format("no suitable rename configuration for field [%s]", oldFieldName));
			}
			Object value = map.remove(oldFieldName);
			map.put(newFieldName, value);
		}
		return map;
	}

	public MigrateFieldRenameProcessor(MigrateFieldRenameProcessorNode migrateFieldRenameProcessorNode) {
		this.tableFieldsMappingMap = migrateFieldRenameProcessorNode.getFieldsMapping().stream()
				.collect(Collectors.toMap(TableFieldInfo::getPreviousTableName,
						t -> t.getFields().stream().collect(Collectors.toMap(FieldInfo::getSourceFieldName, FieldInfo::getTargetFieldName))));
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

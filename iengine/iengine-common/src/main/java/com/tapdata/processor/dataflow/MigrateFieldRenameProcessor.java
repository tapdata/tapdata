package com.tapdata.processor.dataflow;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MigrateFieldRenameProcessor implements DataFlowProcessor{

	private static final Logger logger = LogManager.getLogger(MigrateFieldRenameProcessor.class);

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

		for (Map.Entry<String, FieldInfo> entry : fieldsMappingMap.entrySet()) {
			if (MapUtil.containsKey(map, entry.getKey())) {
				FieldInfo fieldInfo = entry.getValue();
				if (fieldInfo.getIsShow() != null && !fieldInfo.getIsShow()) {
					MapUtil.removeValueByKey(map, entry.getKey());
					continue;
				}
				Object value = MapUtil.getValueByKey(map, entry.getKey());
				try {
					MapUtil.replaceKey(fieldInfo.getSourceFieldName(), map, fieldInfo.getTargetFieldName());
				} catch (Exception e) {
					throw new RuntimeException("Error when modifying field name: " + fieldInfo + "--" + value, e);
				}
			}
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

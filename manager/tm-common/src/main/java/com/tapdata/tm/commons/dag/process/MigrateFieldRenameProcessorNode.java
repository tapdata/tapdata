package com.tapdata.tm.commons.dag.process;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.dag.NodeType;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.SchemaUtils;
import com.tapdata.tm.commons.util.CapitalizedEnum;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.tapdata.tm.commons.base.convert.ObjectIdDeserialize.toObjectId;

@NodeType("migrate_field_rename_processor")
@Getter
@Setter
@Slf4j
public class MigrateFieldRenameProcessorNode extends MigrateProcessorNode {
	/**
	 * 创建处理器节点
	 **/
	public MigrateFieldRenameProcessorNode() {
		super(NodeEnum.migrate_field_rename_processor.name(), NodeCatalog.processor);
	}

	private LinkedList<TableFieldInfo> fieldsMapping;
	private Operation fieldsOperation;

	@Override
	public List<Schema> mergeSchema(List<List<Schema>> inputSchemas, List<Schema> schemas, DAG.Options options) {
		List<Schema> retSchemaList;
		if (null == inputSchemas || inputSchemas.isEmpty()) {
			// return empty list if not found schemas
			retSchemaList = Lists.newArrayList();
		} else {
			// Migrate process node has only one input node
			retSchemaList = inputSchemas.get(0);
			if (null == retSchemaList) {
				retSchemaList = Lists.newArrayList();
			}
		}

		List<Field> fields;
		String ancestorsName;
		ApplyConfig apply = new ApplyConfig(this);
		IOperator<Field> fieldIOperator = new IOperator<Field>() {
			@Override
			public void deleteField(Field field, String originalName) {
				field.setDeleted(true);
			}

			@Override
			public void renameField(Field field, String fromName, String toName) {
				field.setFieldName(toName);
			}
		};
		for (Schema schema : retSchemaList) {
			if (null == schema.getFields()) continue;

			fields = schema.getFields();
			ancestorsName = schema.getAncestorsName();

			for (Field field : fields) {
				apply.apply(ancestorsName, field.getPreviousFieldName(), field, fieldIOperator);
			}
		}

		return retSchemaList;
	}

	@Override
	protected List<Schema> loadSchema(List<String> includes) {
		return null;
	}

	@Override
	protected List<Schema> saveSchema(Collection<String> predecessors, String nodeId, List<Schema> schema, DAG.Options options) {

		schema.forEach(s -> {
			//s.setTaskId(taskId);
			s.setNodeId(nodeId);
		});

		return service.createOrUpdateSchema(ownerId(), toObjectId(getConnectId()), schema, options, this);
	}

	@Override
	protected List<Schema> cloneSchema(List<Schema> schemas) {
		if (schemas == null) {
			return Collections.emptyList();
		}
		return SchemaUtils.cloneSchema(schemas);
	}

	@Override
	public void fieldDdlEvent(TapDDLEvent tapEvent) throws Exception {
		// update field config of rename ddl
		if (tapEvent instanceof TapAlterFieldNameEvent) {
			if (null == fieldsMapping) return;
			ValueChange<String> nameChange = ((TapAlterFieldNameEvent) tapEvent).getNameChange();
			for (TableFieldInfo tableFieldInfo : fieldsMapping) {
				if (!tapEvent.getTableId().equals(tableFieldInfo.getPreviousTableName())) continue;
				if (null == tableFieldInfo.getFields() || tableFieldInfo.getFields().isEmpty()) continue;

				for (FieldInfo fieldInfo : tableFieldInfo.getFields()) {
					if (nameChange.getAfter().equals(fieldInfo.getSourceFieldName())) {
						fieldInfo.setSourceFieldName(nameChange.getBefore());
						return;
					}
				}
			}
		}
	}

	private String getConnectId() {
		AtomicReference<String> connectionId = new AtomicReference<>("");

		getSourceNode().stream().findFirst().ifPresent(node -> connectionId.set(node.getConnectionId()));
		return connectionId.get();
	}

	public interface IOperator<T> {
		default void deleteField(T param, String originalName) {
		}

		default void renameField(T param, String fromName, String toName) {
		}
	}

	public static class ApplyConfig {
		private final Operation fieldsOperation;
		private final Map<String, TableFieldInfo> tableFieldInfoMap;
		private final Map<String, Map<String, FieldInfo>> fieldInfoMaps;


		public ApplyConfig(MigrateFieldRenameProcessorNode node) {
			fieldsOperation = node.getFieldsOperation();
			fieldInfoMaps = new HashMap<>();
			tableFieldInfoMap = Optional.ofNullable(node.getFieldsMapping()).map(tableFieldInfos -> {
				Map<String, TableFieldInfo> tableMap = new HashMap<>();
				for (TableFieldInfo info : tableFieldInfos) {
					tableMap.put(info.getOriginTableName(), info);
					Map<String, FieldInfo> fieldMap = new HashMap<>();
					if (null != info.getFields()) {
						for (FieldInfo fieldInfo : info.getFields()) {
							fieldMap.put(fieldInfo.getSourceFieldName(), fieldInfo);
						}
					}
					fieldInfoMaps.put(info.getOriginTableName(), fieldMap);
				}
				return tableMap;
			}).orElse(new HashMap<>());
		}

		public TableFieldInfo getTableFieldInfo(String tableName) {
			return tableFieldInfoMap.get(tableName);
		}

		public Map<String, FieldInfo> getFieldInfoMap(String tableName) {
			return fieldInfoMaps.get(tableName);
		}

		public FieldInfo getFieldInfo(String tableName, String fieldName) {
			Map<String, FieldInfo> fieldInfoMap = getFieldInfoMap(tableName);
			if (null == fieldInfoMap) return null;
			return fieldInfoMap.get(fieldName);
		}

		public <T> boolean apply(String tableName, String fieldName, T operatorParam, IOperator<T> operator) {
			boolean isShow = true;
			AtomicReference<String> newFieldName = new AtomicReference<>(fieldName);
			newFieldName.set(apply(fieldsOperation, newFieldName.get())); // global settings

			// table settings
			TableFieldInfo tableFieldInfo = getTableFieldInfo(tableName);
			if (null != tableFieldInfo) {
				newFieldName.set(apply(tableFieldInfo.getOperation(), newFieldName.get()));

				// field settings
				FieldInfo fieldInfo = getFieldInfo(tableName, fieldName);
				if (null != fieldInfo) {
					if (Boolean.FALSE.equals(fieldInfo.getIsShow())) {
						operator.deleteField(operatorParam, fieldName);
						isShow = false;
					}
					if (StringUtils.isNotBlank(fieldInfo.getTargetFieldName())) {
						newFieldName.set(fieldInfo.getTargetFieldName());
					}
				}
			}

			if (!fieldName.equals(newFieldName.get())) {
				operator.renameField(operatorParam, fieldName, newFieldName.get());
			}

			return isShow;
		}

		private String apply(Operation operation, String originalFieldName) {
			String resValue = originalFieldName;
			if (null == operation) return resValue;

			if (StringUtils.isNotBlank(operation.getPrefix())) {
				resValue = operation.getPrefix().concat(resValue);
			}
			if (StringUtils.isNotBlank(operation.getSuffix())) {
				resValue = resValue.concat(operation.getSuffix());
			}
			if (StringUtils.isNotBlank(operation.getCapitalized())) {
				switch (CapitalizedEnum.fromValue(operation.getCapitalized())) {
					case UPPER:
						resValue = StringUtils.upperCase(resValue);
						break;
					case LOWER:
						resValue = StringUtils.lowerCase(resValue);
						break;
					case SNAKE:
						resValue = StrUtil.toUnderlineCase(resValue);
						break;
					case CAMEL:
						resValue = StrUtil.toCamelCase(resValue);
						break;
					default:
						break;

				}
			}
			return resValue;
		}
	}
}


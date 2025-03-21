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
import com.tapdata.tm.commons.util.CapitalizedEnum;
import com.tapdata.tm.commons.util.PartitionTableFieldRenameOperator;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.schema.TapConstraint;
import io.tapdata.entity.schema.TapConstraintMapping;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
		IOperator<List<TapConstraint>> foreignKeyConstraintIOperator = createForeignKeyConstraintIOperator();
		IOperator<List<TapConstraint>> referenceForeignKeyConstraintIOperator = createReferenceForeignKeyConstraintIOperator();
		for (Schema schema : retSchemaList) {
			if (null == schema.getFields()) continue;

			fields = schema.getFields();
			String tableName = schema.getOriginalName();
			PartitionTableFieldRenameOperator operator = apply.startPartitionTableFieldRename();
			for (Field field : fields) {
				apply.apply(tableName, field.getPreviousFieldName(), field, fieldIOperator);
				handleForeignKeyConstraints(schema, field, apply, tableName, foreignKeyConstraintIOperator);
				handleReferenceForeignKeyConstraints(schema, apply, referenceForeignKeyConstraintIOperator);
			}
			operator.endOf(schema);
		}

		return retSchemaList;
	}

	protected void handleReferenceForeignKeyConstraints(Schema schema, ApplyConfig apply, IOperator<List<TapConstraint>> referenceForeignKeyConstraintIOperator) {
		List<TapConstraint> constraints = schema.getConstraints();
		CopyOnWriteArrayList<TapConstraint> tapConstraints = new CopyOnWriteArrayList<>(constraints);
		if (CollectionUtils.isNotEmpty(constraints)) {
			for (TapConstraint constraint : tapConstraints) {
				String referencesTableName = constraint.getReferencesTableName();
				List<TapConstraintMapping> mappingFields = constraint.getMappingFields();
				for (TapConstraintMapping mappingField : mappingFields) {
					String referenceKey = mappingField.getReferenceKey();
					apply.apply(referencesTableName, referenceKey, constraints, referenceForeignKeyConstraintIOperator);
				}
			}
		}
		schema.setConstraints(new ArrayList<>(tapConstraints));
	}

	protected void handleForeignKeyConstraints(Schema schema, Field field, ApplyConfig apply, String tableName,
													  IOperator<List<TapConstraint>> createForeignKeyConstraintIOperator) {
		List<TapConstraint> constraints = schema.getConstraints();
		if(CollectionUtils.isNotEmpty(constraints)) {
			apply.apply(tableName, field.getOriginalFieldName(), constraints, createForeignKeyConstraintIOperator);
		}
	}

	protected IOperator<List<TapConstraint>> createForeignKeyConstraintIOperator() {
		return new IOperator<List<TapConstraint>>() {
			@Override
			public void renameField(List<TapConstraint> param, String fromName, String toName) {
				param.forEach(tapConstraint -> {
					if (tapConstraint.getType().equals(TapConstraint.ConstraintType.FOREIGN_KEY)) {
						List<TapConstraintMapping> mappingFields = tapConstraint.getMappingFields();
						mappingFields.forEach(mappingField -> {
							if (mappingField.getForeignKey().equals(fromName)) {
								mappingField.foreignKey(toName);
							}
						});
					}
				});
			}

			@Override
			public void deleteField(List<TapConstraint> param, String originalName) {
				List<TapConstraint> needRemove = new ArrayList<>();
				param.forEach(tapConstraint -> {
					if (tapConstraint.getType().equals(TapConstraint.ConstraintType.FOREIGN_KEY)) {
						List<TapConstraintMapping> mappingFields = tapConstraint.getMappingFields();
						if (null != mappingFields.stream().filter(mappingField -> mappingField.getForeignKey().equals(originalName)).findFirst().orElse(null)) {
							if (!needRemove.contains(tapConstraint)) {
								needRemove.add(tapConstraint);
							}
						}
					}
				});
				if (CollectionUtils.isNotEmpty(needRemove)) {
					needRemove.forEach(param::remove);
				}
			}
		};
	}

	protected IOperator<List<TapConstraint>> createReferenceForeignKeyConstraintIOperator() {
		return new IOperator<List<TapConstraint>>() {
			@Override
			public void deleteField(List<TapConstraint> param, String originalName) {
				List<TapConstraint> needRemove = new ArrayList<>();
				param.forEach(tapConstraint -> {
					if (tapConstraint.getType().equals(TapConstraint.ConstraintType.FOREIGN_KEY)) {
						List<TapConstraintMapping> mappingFields = tapConstraint.getMappingFields();
						if (null != mappingFields.stream().filter(mappingField -> mappingField.getReferenceKey().equals(originalName)).findFirst().orElse(null)) {
							if (!needRemove.contains(tapConstraint)) {
								needRemove.add(tapConstraint);
							}
						}
					}
				});
				if (CollectionUtils.isNotEmpty(needRemove)) {
					needRemove.forEach(param::remove);
				}
			}

			@Override
			public void renameField(List<TapConstraint> param, String fromName, String toName) {
				param.forEach(tapConstraint -> {
					if (tapConstraint.getType().equals(TapConstraint.ConstraintType.FOREIGN_KEY)) {
						List<TapConstraintMapping> mappingFields = tapConstraint.getMappingFields();
						mappingFields.forEach(mappingField -> {
							if (mappingField.getReferenceKey().equals(fromName)) {
								mappingField.referenceKey(toName);
							}
						});
					}
				});
			}
		};
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



	public interface IOperator<T> {
		default void deleteField(T param, String originalName) {
		}

		default void renameField(T param, String fromName, String toName) {
		}

		default void renameField(String oldKey, String newKey, Map<String, Object> originValueMap, T param) {
		}

		default Object renameFieldWithReturn(T param, String fromName, String toName) {
			return null;
		}
	}

	public static class ApplyConfig {
		protected final Operation fieldsOperation;
		protected final Map<String, TableFieldInfo> tableFieldInfoMap;
		protected final Map<String, Map<String, FieldInfo>> fieldInfoMaps;
		protected final PartitionTableFieldRenameOperator partitionTableFieldRenameOperator;
		protected final Map<String, List<String>> targetFieldExistMaps;

		public ApplyConfig(MigrateFieldRenameProcessorNode node) {
			fieldsOperation = node.getFieldsOperation();
			fieldInfoMaps = new HashMap<>();
			targetFieldExistMaps = new HashMap<>();
			tableFieldInfoMap = Optional.ofNullable(node.getFieldsMapping()).map(tableFieldInfos -> {
				Map<String, TableFieldInfo> tableMap = new HashMap<>();
				for (TableFieldInfo info : tableFieldInfos) {
					tableMap.put(info.getPreviousTableName(), info);
					Map<String, FieldInfo> fieldMap = new HashMap<>();
					List<String> sourceFieldNames = new ArrayList<>();
					List<String> targetFieldNames = new ArrayList<>();
					if (null != info.getFields()) {
						for (FieldInfo fieldInfo : info.getFields()) {
							fieldMap.put(fieldInfo.getSourceFieldName(), fieldInfo);
							String sourceFieldName = fieldInfo.getSourceFieldName();
							String targetFieldName = fieldInfo.getTargetFieldName();
							sourceFieldNames.add(sourceFieldName);
							if (null != sourceFieldName && !sourceFieldName.equals(targetFieldName)) {
								targetFieldNames.add(targetFieldName);
							}
						}
					}
					List<String> targetFieldExists = targetFieldNames.stream().filter(fieldName -> sourceFieldNames.contains(fieldName)).collect(Collectors.toList());
					fieldInfoMaps.put(info.getPreviousTableName(), fieldMap);
					targetFieldExistMaps.put(info.getPreviousTableName(), targetFieldExists);
				}
				return tableMap;
			}).orElse(new HashMap<>());
			partitionTableFieldRenameOperator = new PartitionTableFieldRenameOperator();
		}

		public PartitionTableFieldRenameOperator startPartitionTableFieldRename() {
			partitionTableFieldRenameOperator.startAt();
			return partitionTableFieldRenameOperator;
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
				partitionTableFieldRenameOperator.rename(fieldName, newFieldName.get());
				operator.renameField(operatorParam, fieldName, newFieldName.get());
			}

			return isShow;
		}

		protected String apply(Operation operation, String originalFieldName) {
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


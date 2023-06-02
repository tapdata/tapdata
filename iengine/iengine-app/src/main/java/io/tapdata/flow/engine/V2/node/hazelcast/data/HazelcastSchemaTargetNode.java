package io.tapdata.flow.engine.V2.node.hazelcast.data;


import com.hazelcast.jet.core.Inbox;
import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.schema.SchemaApplyResult;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.util.TapModelDeclare;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.CustomProcessorNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.schema.Schema;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.error.VirtualTargetExCode_14;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.script.ObsScriptLogger;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.schema.TapTableMap;
import io.tapdata.schema.TapTableUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.jetbrains.annotations.NotNull;
import org.voovan.tools.collection.CacheMap;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class HazelcastSchemaTargetNode extends HazelcastVirtualTargetNode {

	/**
	 * key: subTaskId+jsNodeId
	 */
	private static final CacheMap<String, TapTable> tabTableCacheMap = new CacheMap<>();
	private static final CacheMap<String, List<SchemaApplyResult>> schemaApplyResultMap = new CacheMap<>();

	private static final CacheMap<String, Supplier<TapTable>> defaultTapTableSupplierMap = new CacheMap<>();

	private static final CacheMap<String, Supplier<List<SchemaApplyResult>>> defaultSchemaApplyResultSupplierMap = new CacheMap<>();
	private static final CacheMap<String, Function<Object, Object>> defaultDeclareFunctionMap = new CacheMap<>();
	public static final String FUNCTION_NAME_DECLARE = "declare";

	private final String schemaKey;
	private final TapTableMap<String, TapTable> oldTapTableMap;

	private boolean needToDeclare;

	private Function<Object, Object> declareFunction;

	static {
		tabTableCacheMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
		schemaApplyResultMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
		defaultTapTableSupplierMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
		defaultSchemaApplyResultSupplierMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
		defaultDeclareFunctionMap.maxSize(100).autoRemove(true).expire(600).interval(60).create();
	}


	public static TapTable getTapTable(String schemaKey) {
		TapTable tapTable = tabTableCacheMap.remove(schemaKey);
		if (tapTable == null) {
			Supplier<TapTable> tapTableSupplier = defaultTapTableSupplierMap.remove(schemaKey);
			if (tapTableSupplier != null) {
				return tapTableSupplier.get();
			}
		}
		return tapTable;
	}

	public static List<SchemaApplyResult> getSchemaApplyResultList(String schemaKey) {
		List<SchemaApplyResult> schemaApplyResults = schemaApplyResultMap.remove(schemaKey);
		if (schemaApplyResults == null) {
			Supplier<List<SchemaApplyResult>> schemaApplyResultsSupplier = defaultSchemaApplyResultSupplierMap.remove(schemaKey);
			if (schemaApplyResultsSupplier != null) {
				return schemaApplyResultsSupplier.get();
			}
		}
		return schemaApplyResults;
	}


	public HazelcastSchemaTargetNode(DataProcessorContext dataProcessorContext) throws Exception {
		super(dataProcessorContext);
		this.schemaKey = dataProcessorContext.getTaskDto().getId().toHexString() + "-" + dataProcessorContext.getNode().getId();

		List<Node<Schema>> preNodes = getNode().predecessors();
		if (preNodes.size() != 1) {
			throw new IllegalArgumentException("HazelcastSchemaTargetNode only allows one predecessor node");
		}
		Node<?> deductionSchemaNode = preNodes.get(0);
		this.oldTapTableMap = TapTableUtil.getTapTableMap("predecessor_" + getNode().getId() + "_", deductionSchemaNode, null);

		if (deductionSchemaNode instanceof JsProcessorNode
				|| deductionSchemaNode instanceof MigrateJsProcessorNode
				|| deductionSchemaNode instanceof CustomProcessorNode) {
			String declareScript = (String) ReflectionUtil.getFieldValue(deductionSchemaNode, "declareScript");
			this.needToDeclare = StringUtils.isNotEmpty(declareScript);
			if (this.needToDeclare) {
				this.declareFunction = (param) -> {
					Invocable engine = null;
					try {
						String realDeclareScript;
						if (multipleTables) {
							realDeclareScript = String.format("function declare(schemaApplyResultList){\n %s \n return schemaApplyResultList;\n}", declareScript);
						} else {
							realDeclareScript = String.format("function declare(tapTable){\n %s \n return tapTable;\n}", declareScript);
						}
						ObsScriptLogger scriptLogger = new ObsScriptLogger(obsLogger);
						engine = ScriptUtil.getScriptEngine(realDeclareScript, null, null,
								((DataProcessorContext) processorBaseContext).getCacheService(), scriptLogger
						);
						TapModelDeclare tapModelDeclare = new TapModelDeclare(scriptLogger);
						((ScriptEngine) engine).put("TapModelDeclare", tapModelDeclare);
						return engine.invokeFunction(FUNCTION_NAME_DECLARE, param);
					} catch (Throwable throwable) {
						throw new RuntimeException("Error executing declaration code", throwable);
					} finally {
						if (engine instanceof GraalJSScriptEngine) {
							((GraalJSScriptEngine) engine).close();
						}
					}
				};
				defaultDeclareFunctionMap.put(schemaKey, declareFunction);
				if (multipleTables) {
					defaultSchemaApplyResultSupplierMap.put(schemaKey, () -> (List<SchemaApplyResult>) declareFunction.apply(new ArrayList<>()));
				} else {
					defaultTapTableSupplierMap.put(schemaKey, () -> {
						List<TapTable> tapTables = TapTableUtil.getTapTables(deductionSchemaNode);
						if (CollectionUtils.isNotEmpty(tapTables)) {
							return (TapTable) declareFunction.apply(tapTables.get(0));
						}
						return (TapTable) declareFunction.apply(new TapTable());
					});
				}
			}
		}
	}

	@Override
	public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {

						TapRecordEvent tapEvent;
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							if (obsLogger.isDebugEnabled()) {
								obsLogger.debug("tapdata event [{}]", tapdataEvent.toString());
							}
							if (null != tapdataEvent.getMessageEntity()) {
								tapEvent = message2TapEvent(tapdataEvent.getMessageEntity());
							} else if (null != tapdataEvent.getTapEvent()) {
								tapEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
							} else {
								continue;
							}
							// 解析模型
							TapTable tapTable = getNewTapTable(tapEvent);
							try {
								if (!multipleTables) {
									//迁移任务，只有一张表
									if (needToDeclare) {
										tapTable = (TapTable) this.declareFunction.apply(tapTable);
									}
									tabTableCacheMap.put(schemaKey, tapTable);
								}

								if (multipleTables) {
									// 获取差异模型
									List<SchemaApplyResult> schemaApplyResults = getSchemaApplyResults(tapTable);
									if (needToDeclare) {
										schemaApplyResults = (List<SchemaApplyResult>) this.declareFunction.apply(schemaApplyResults);
									}
									schemaApplyResultMap.put(schemaKey, schemaApplyResults);
								}
							} catch (Exception e) {
								String msg = String.format(" tableName: %s, %s", tapTable.getId(), e.getMessage());
								throw new TapCodeException(VirtualTargetExCode_14.DECLARE_ERROR, msg, e);
							}
						}

					} else {
						break;
					}
				}
			}
		} catch (TapCodeException tapCodeException) {
			throw tapCodeException;
		} catch (Throwable e) {
			throw new TapCodeException(VirtualTargetExCode_14.UNKNOWN_ERROR, e);
		} finally {
			ThreadContext.clearAll();
		}
	}

	@Override
	protected void doClose() throws Exception {
		super.doClose();
		CommonUtils.ignoreAnyError(() -> Optional.ofNullable(this.oldTapTableMap).ifPresent(TapTableMap::reset), HazelcastSchemaTargetNode.class.getSimpleName());
	}

	@NotNull
	private TapTable getNewTapTable(TapRecordEvent tapEvent) {
		Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
		if (obsLogger.isDebugEnabled()) {
			obsLogger.debug("after map is [{}]", after);
		}
		TapTable tapTable = new TapTable(tapEvent.getTableId());
		if (MapUtils.isNotEmpty(after)) {
			LinkedHashMap<String, TapField> oldNameFieldMap = getOldNameFieldMap(tapEvent.getTableId());
			for (Map.Entry<String, Object> entry : after.entrySet()) {
				String fieldName = entry.getKey();
				if (obsLogger.isDebugEnabled()) {
					obsLogger.debug("entry type: {} - {}", fieldName, entry.getValue().getClass());
				}
				TapType tapType;
				if (entry.getValue() instanceof TapValue) {
					TapValue<?, ?> tapValue = (TapValue<?, ?>) entry.getValue();
					tapType = tapValue.getTapType();
				} else {
					tapType = JavaTypesToTapTypes.toTapType(entry.getValue());
					if (tapType == null) {
						tapType = TapSimplify.tapRaw();
					}
				}
				TapField tapField = null;
				if (oldNameFieldMap != null) {
					TapField oldTapField = oldNameFieldMap.get(fieldName);
					if (oldTapField != null && oldTapField.getTapType() != null
							&& (oldTapField.getTapType().getType() == tapType.getType() || tapType.getType() == TapType.TYPE_RAW)) {
						tapField = oldTapField;
					}
				}
				if (tapField == null) {
					tapField = new TapField().name(fieldName).tapType(tapType);
				}
				tapTable.add(tapField);
			}
		}

		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isNotEmpty(nameFieldMap)) {
			Set<String> fieldNames = nameFieldMap.keySet();
			List<TapIndex> oldTapIndexList = getOldTapIndexList(tapEvent.getTableId());
			if (CollectionUtils.isNotEmpty(oldTapIndexList)) {
				for (TapIndex oldTapIndex : oldTapIndexList) {
					List<TapIndexField> oldIndexFields = oldTapIndex.getIndexFields();
					List<TapIndexField> newIndexFields = new ArrayList<>();
					for (TapIndexField oldIndexField : oldIndexFields) {
						if (fieldNames.contains(oldIndexField.getName())) {
							newIndexFields.add(oldIndexField);
						}
					}
					if (CollectionUtils.isNotEmpty(newIndexFields)) {
						oldTapIndex.setIndexFields(newIndexFields);
						tapTable.add(oldTapIndex);
					}
				}
			}
		}
		return tapTable;
	}

	@NotNull
	private List<SchemaApplyResult> getSchemaApplyResults(TapTable tapTable) {
		List<SchemaApplyResult> schemaApplyResults = new ArrayList<>();

		LinkedHashMap<String, TapField> newNameFieldMap = tapTable.getNameFieldMap();
		LinkedHashMap<String, TapField> oldNameFieldMap = getOldNameFieldMap(tapTable.getName());
		if (MapUtils.isNotEmpty(newNameFieldMap) && MapUtils.isNotEmpty(oldNameFieldMap)) {
			for (Map.Entry<String, TapField> entry : newNameFieldMap.entrySet()) {
				String newFieldName = entry.getKey();
				TapField newTapField = entry.getValue();
				if (!oldNameFieldMap.containsKey(newFieldName)) {
					//create field
					schemaApplyResults.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CREATE, newFieldName, newTapField));
					continue;
				}
				TapField oldTapField = oldNameFieldMap.get(newFieldName);
				if (!oldTapField.getTapType().equals(newTapField.getTapType())) {
					//alter field
					schemaApplyResults.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_CONVERT, newFieldName, newTapField));
				}
			}

			for (String oldFieldName : oldNameFieldMap.keySet()) {
				if (!newNameFieldMap.containsKey(oldFieldName)) {
					//drop field
					schemaApplyResults.add(new SchemaApplyResult(SchemaApplyResult.OP_TYPE_REMOVE, oldFieldName, null));
				}
			}
		}

		return schemaApplyResults;
	}

	private LinkedHashMap<String, TapField> getOldNameFieldMap(String tableName) {
		if (oldTapTableMap == null || !oldTapTableMap.containsKey(tableName)) {
			return null;
		}
		return oldTapTableMap.get(tableName).getNameFieldMap();
	}

	private List<TapIndex> getOldTapIndexList(String tableName) {
		if (oldTapTableMap == null || !oldTapTableMap.containsKey(tableName)) {
			return null;
		}
		return oldTapTableMap.get(tableName).getIndexList();
	}

}

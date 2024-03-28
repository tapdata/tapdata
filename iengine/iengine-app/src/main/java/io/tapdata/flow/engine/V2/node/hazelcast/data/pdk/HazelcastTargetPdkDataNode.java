package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.google.common.collect.Maps;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.ExistsDataProcessEnum;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.flow.engine.V2.exactlyonce.ExactlyOnceUtil;
import io.tapdata.flow.engine.V2.exception.TapExactlyOnceWriteExCode_22;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastTargetPdkDataNode extends HazelcastTargetPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	public static final int MAX_INDEX_FIELDS_COUNT = 10;
	public static final int MAX_RECORD_OBS_WARN = 3;
	public static final int CREATE_INDEX_THRESHOLD = 5000000;
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkDataNode.class);
	private ClassHandlers ddlEventHandlers;

	public HazelcastTargetPdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		try {
			super.doInit(context);
			if (getNode() instanceof TableNode) {
				TableNode tableNode = (TableNode) getNode();
				lastTableName = tableNode.getTableName();
				updateConditionFieldsMap.put(lastTableName, tableNode.getUpdateConditionFields());
			} else if (getNode() instanceof DatabaseNode) {
				DatabaseNode dbNode = (DatabaseNode) getNode();
				if (Objects.isNull(dbNode.getUpdateConditionFieldMap())) {
					dbNode.setUpdateConditionFieldMap(Maps.newHashMap());
				}
				updateConditionFieldsMap.putAll(dbNode.getUpdateConditionFieldMap());
			}
			if (getNode() instanceof DataParentNode) {
				writeStrategy = ((DataParentNode<?>) getNode()).getWriteStrategy();
			}
			initTargetDB();
		} catch (Exception e) {
			Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			} else {
				throw new TapCodeException(TaskTargetProcessorExCode_15.UNKNOWN_ERROR, e);
			}
		}
		initDDLHandler();
	}


	private void initDDLHandler() {
		ddlEventHandlers = new ClassHandlers();
		ddlEventHandlers.register(TapNewFieldEvent.class, this::executeNewFieldFunction);
		ddlEventHandlers.register(TapAlterFieldNameEvent.class, this::executeAlterFieldNameFunction);
		ddlEventHandlers.register(TapAlterFieldAttributesEvent.class, this::executeAlterFieldAttrFunction);
		ddlEventHandlers.register(TapDropFieldEvent.class, this::executeDropFieldFunction);
		ddlEventHandlers.register(TapCreateTableEvent.class, this::executeCreateTableFunction);
		ddlEventHandlers.register(TapCreateIndexEvent.class, this::executeCreateIndexFunction);
		ddlEventHandlers.register(TapDropTableEvent.class, tapDropTableEvent -> {
			// only execute start function aspect so that it would be cheated as input
			AspectUtils.executeAspect(new DropTableFuncAspect()
					.dropTableEvent(tapDropTableEvent)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext).state(DropTableFuncAspect.STATE_START));
			obsLogger.warn("Drop table event will be ignored at sink node: " + tapDropTableEvent.getTableId());
			return null;
		});
	}

	private void initTargetDB() {
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		executeDataFuncAspect(TableInitFuncAspect.class, () -> new TableInitFuncAspect()
				.tapTableMap(tapTableMap)
				.dataProcessorContext(dataProcessorContext)
				.start(), (funcAspect -> {
			Node<?> node = dataProcessorContext.getNode();
			ExistsDataProcessEnum existsDataProcessEnum = getExistsDataProcess(node);
			SyncProgress syncProgress = foundSyncProgress(dataProcessorContext.getTaskDto().getAttrs());
			if (null == syncProgress) {
				for (String tableId : tapTableMap.keySet()) {
					if (!isRunning()) {
						return;
					}
					createTable(tapTableMap, funcAspect, node, existsDataProcessEnum, tableId);
				}
			}

			//对于复制任务停下来后新增表的建表。
			TaskDto taskDto = dataProcessorContext.getTaskDto();
			if (taskDto != null && TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
				List<String> ldpNewTables = taskDto.getLdpNewTables();
				if (CollectionUtils.isNotEmpty(ldpNewTables)) {
					DAG dag = taskDto.getDag();
					if (dag != null) {
						LinkedList<DatabaseNode> targetNode = dag.getTargetNode();
						if (CollectionUtils.isNotEmpty(targetNode)) {
							DatabaseNode last = targetNode.getLast();
							if (last != null) {
								Map<String, String> sourceAndTargetMap = new HashMap<>();
								List<SyncObjects> syncObjects = last.getSyncObjects();
								SyncObjects syncObjects1 = syncObjects.get(0);
								LinkedHashMap<String, String> tableNameRelation = syncObjects1.getTableNameRelation();
								if (tableNameRelation != null && tableNameRelation.size() > 0) {
									sourceAndTargetMap = syncObjects1.getTableNameRelation();
								}
								for (String ldpNewTable : ldpNewTables) {
									String tableId = sourceAndTargetMap.get(ldpNewTable);
									if (tableId != null) {
										if (!isRunning()) {
											return;
										}
										createTable(tapTableMap, funcAspect, node, existsDataProcessEnum, tableId);
									}
								}
							}
						}
					}
				}
			}
		}));
	}

	protected void createTable(TapTableMap<String, TapTable> tapTableMap, TableInitFuncAspect funcAspect, Node<?> node, ExistsDataProcessEnum existsDataProcessEnum, String tableId) {
		TapTable tapTable = tapTableMap.get(tableId);
		List<String> updateConditionFields = getUpdateConditionFields(node, tapTable);
		if (null == tapTable) {
			TapCodeException e = new TapCodeException(TaskTargetProcessorExCode_15.INIT_TARGET_TABLE_TAP_TABLE_NULL, "Table name: " + tableId);
			if (null != funcAspect) funcAspect.setThrowable(e);
			throw e;
		}
		if (StringUtils.isNotBlank(tableId) && StringUtils.equalsAny(tableId, ExactlyOnceUtil.EXACTLY_ONCE_CACHE_TABLE_NAME)) {
			return;
		}
		dropTable(existsDataProcessEnum, tableId);
//		boolean createUnique = tapTable.getIndexList() != null && tapTable.getIndexList().stream().anyMatch(idx -> !idx.isPrimary() && idx.isUnique() && (idx.getIndexFields().size() == updateConditionFields.size()) &&
//				(idx.getIndexFields().stream().allMatch(idxField -> updateConditionFields.contains(idxField.getName()))));
		AtomicBoolean succeed = new AtomicBoolean(false);
		boolean createdTable = createTable(tapTable, succeed);
		clearData(existsDataProcessEnum, tableId);
//		createUnique &= succeed.get();
		createTargetIndex(updateConditionFields, succeed.get(), tableId, tapTable, createdTable);
		//sync index
		syncIndex(tableId, tapTable, succeed.get());
		if (null != funcAspect)
			funcAspect.state(TableInitFuncAspect.STATE_PROCESS).completed(tableId, createdTable);
	}

	private List<String> getUpdateConditionFields(Node<?> node, TapTable tapTable) {
		if (node instanceof TableNode) {
			return ((TableNode) node).getUpdateConditionFields();
		} else if (node instanceof DatabaseNode) {
			Map<String, List<String>> updateConditionFieldMap = ((DatabaseNode) node).getUpdateConditionFieldMap();
			return updateConditionFieldMap.computeIfAbsent(tapTable.getId(), s -> new ArrayList<>(tapTable.primaryKeys(true)));
		} else {
			return null;
		}
	}

	private void createTargetIndex(List<String> updateConditionFields, boolean createUnique, String tableId, TapTable tapTable, boolean createdTable) {

		if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.appendWrite.name())) {
			return;
		}
		CreateIndexFunction createIndexFunction = getConnectorNode().getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			return;
		}

		AtomicReference<TapCreateIndexEvent> indexEvent = new AtomicReference<>();
		try {
			List<TapIndex> tapIndices = new ArrayList<>();
			TapIndex tapIndex = new TapIndex().unique(createUnique);
			List<TapIndexField> tapIndexFields = new ArrayList<>();
			if (null == updateConditionFields) {
				obsLogger.warn("Table " + tableId + " index fields is null, will not create index automatically");
				return;
			}
			if (updateConditionFields.size() > MAX_INDEX_FIELDS_COUNT) {
				obsLogger.warn("Table " + tableId + " index field exceeds the maximum value of 10, the index will not be created automatically, please create it manually");
				return;
			}
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				boolean usePkAsUpdateConditions = usePkAsUpdateConditions(updateConditionFields, tapTable.primaryKeys());
				if (usePkAsUpdateConditions && createdTable) {
//					obsLogger.info("Table " + tableId + " use the primary key as the update condition, which is created when the table is create, and ignored");
					return;
				}
				updateConditionFields.forEach(field -> {
					TapIndexField tapIndexField = new TapIndexField();
					tapIndexField.setName(field);
					tapIndexField.setFieldAsc(true);
					tapIndexFields.add(tapIndexField);
				});
				tapIndex.setIndexFields(tapIndexFields);
				tapIndices.add(tapIndex);
				indexEvent.set(createIndexEvent(tableId, tapIndices));

				executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
						.table(tapTable)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.createIndexEvent(indexEvent.get())
						.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
						PDKMethod.TARGET_CREATE_INDEX,
						() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, indexEvent.get()), TAG));
			}
		} catch (Throwable throwable) {
			Throwable matched = CommonUtils.matchThrowable(throwable, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED, "Table name: " + tableId, throwable)
						.addEvent(indexEvent.get());
			}
		}
	}
	protected void syncIndex(String tableId, TapTable tapTable, boolean autoCreateTable){
		long start = System.currentTimeMillis();
		if (!checkSyncIndexOpen()) return;
		if (!autoCreateTable) {
			obsLogger.warn("Table: {} already exists and will no longer synchronize indexes",tableId);
			return;
		}
		CreateIndexFunction createIndexFunction = getConnectorNode().getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			obsLogger.warn("target connector does not support create index and will no longer synchronize indexes");
			return;
		}
		GetTableInfoFunction getTableInfoFunction = getConnectorNode().getConnectorFunctions().getGetTableInfoFunction();
		if (null == getTableInfoFunction){
			obsLogger.warn("target connector does not support get table information and will no longer synchronize indexes");
			return;
		}
		QueryIndexesFunction queryIndexesFunction = getConnectorNode().getConnectorFunctions().getQueryIndexesFunction();
		if (null == queryIndexesFunction){
			obsLogger.warn("target connector does not support query index and will no longer synchronize indexes");
			return;
		}
		AtomicReference<TapCreateIndexEvent> indexEvent = new AtomicReference<>();
		try {
			//query table info
			TableInfo tableInfo = getTableInfoFunction.getTableInfo(getConnectorNode().getConnectorContext(), tableId);
			if (null != tableInfo) {
				if (null == tableInfo.getNumOfRows()){
					obsLogger.warn("Table: {} records amount is unknown and will no longer synchronize indexes",tableId);
					return;
				}
				if (tableInfo.getNumOfRows() > CREATE_INDEX_THRESHOLD){
					obsLogger.warn("Table: {} records amount exceeds the threshold: {} for creating indexes and will no longer synchronize indexes",tableId,CREATE_INDEX_THRESHOLD);
					return;
				}
			}else {
				obsLogger.warn("Table: {} gets table information failed and will no longer synchronize indexes",tableId);
				return;
			}
			List<TapIndex> indexList = new ArrayList<>();
			List<TapIndex> indices = tapTable.getIndexList();
			indices.forEach(index -> {
				TapIndex tapIndex = new TapIndex().unique(index.getUnique()).primary(index.getPrimary());
				tapIndex.setIndexFields(index.getIndexFields());
				tapIndex.setName(index.getName());
				tapIndex.setCluster(index.getCluster());
				indexList.add(tapIndex);
			});
			//query exist index
			queryIndexesFunction.query(getConnectorNode().getConnectorContext(), tapTable, (tapIndexList)->{
				tapIndexList.forEach(existsIndex -> {
					// If the index already exists, it will no longer be created; Having the same name is considered as existence; Fields with the same order are also considered to exist
					for (TapIndex tapIndex : indexList) {
						if (tapIndex.getName().equals(existsIndex.getName())) {
							indexList.remove(tapIndex);
							obsLogger.info("Table: {} already exists Index: {} and will no longer create index", tableId, tapIndex.getName());
							break;
						}
						if (tapIndex.getIndexFields().size() == existsIndex.getIndexFields().size()) {
							boolean same = true;
							for (int i = 0; i < tapIndex.getIndexFields().size(); i++) {
								if (!tapIndex.getIndexFields().get(i).getName().equals(existsIndex.getIndexFields().get(i).getName())
										|| !Objects.equals(tapIndex.getIndexFields().get(i).getFieldAsc(), existsIndex.getIndexFields().get(i).getFieldAsc())) {
									same = false;
									break;
								}
							}
							if (same) {
								indexList.remove(tapIndex);
								obsLogger.info("Table: {} already exists Index: {} and will no longer create index", tableId, tapIndex.getName());
								break;
							}
						}
					}
				});
			});
			if (CollectionUtils.isEmpty(indexList)) {
				obsLogger.info("Table: {} already exists Index list: {}", tableId, indices);
				return;
			}
			indexEvent.set(createIndexEvent(tableId, indexList));
			obsLogger.info("Table: {} will create Index list: {}", indexEvent.get().getTableId(), indexList);
			executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
					.table(tapTable)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext)
					.createIndexEvent(indexEvent.get())
					.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
					PDKMethod.TARGET_CREATE_INDEX,
					() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, indexEvent.get()), TAG));
		}catch (Throwable throwable){
			Throwable matched = CommonUtils.matchThrowable(throwable, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED, "Table name: " + tableId, throwable)
						.addEvent(indexEvent.get());
			}
		}
		long end = System.currentTimeMillis();
		obsLogger.info("Table: {} synchronize indexes completed, cost {}ms", tableId, end-start);
	}
	protected boolean checkSyncIndexOpen(){
		Node node = getNode();
		if (node instanceof DatabaseNode || node instanceof TableNode) {
			DataParentNode dataParentNode = (DataParentNode) node;
			if (Boolean.TRUE.equals(dataParentNode.getSyncIndexEnable())) {
				return true;
			}
		}
		return false;
	}

	private void clearData(ExistsDataProcessEnum existsDataProcessEnum, String tableId) {
		if (SyncTypeEnum.CDC == syncType || existsDataProcessEnum != ExistsDataProcessEnum.REMOVE_DATE) return;

		AtomicReference<TapClearTableEvent> tapClearTableEvent = new AtomicReference<>();
		try {
			ClearTableFunction clearTableFunction = getConnectorNode().getConnectorFunctions().getClearTableFunction();
			Optional.ofNullable(clearTableFunction).ifPresent(func -> {
				tapClearTableEvent.set(clearTableEvent(tableId));
				executeDataFuncAspect(ClearTableFuncAspect.class, () -> new ClearTableFuncAspect()
						.clearTableEvent(tapClearTableEvent.get())
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), clearTableFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_CLEAR_TABLE, () -> func.clearTable(getConnectorNode().getConnectorContext(), tapClearTableEvent.get()), TAG));
			});
		} catch (Throwable throwable) {
			Throwable matched = CommonUtils.matchThrowable(throwable, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.CLEAR_TABLE_FAILED, "Table name: " + tableId, throwable)
						.addEvent(tapClearTableEvent.get());
			}
		}
	}

	private void dropTable(ExistsDataProcessEnum existsDataProcessEnum, String tableId) {
		if (SyncTypeEnum.CDC == syncType || existsDataProcessEnum != ExistsDataProcessEnum.DROP_TABLE) return;

		AtomicReference<TapDropTableEvent> tapDropTableEvent = new AtomicReference<>();
		try {
			DropTableFunction dropTableFunction = getConnectorNode().getConnectorFunctions().getDropTableFunction();
			if (dropTableFunction != null) {
				tapDropTableEvent.set(dropTableEvent(tableId));
				executeDataFuncAspect(DropTableFuncAspect.class, () -> new DropTableFuncAspect()
						.dropTableEvent(tapDropTableEvent.get())
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), (dropTableFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_DROP_TABLE, () -> dropTableFunction.dropTable(getConnectorNode().getConnectorContext(), tapDropTableEvent.get()), TAG)));
			} else {
				// only execute start function aspect so that it would be cheated as input
				AspectUtils.executeAspect(new DropTableFuncAspect()
						.dropTableEvent(tapDropTableEvent.get())
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext).state(NewFieldFuncAspect.STATE_START));
			}
		} catch (Throwable throwable) {
			Throwable matched = CommonUtils.matchThrowable(throwable, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.DROP_TABLE_FAILED, "Table name: " + tableId, throwable)
						.addEvent(tapDropTableEvent.get());
			}
		}
	}

	@Nullable
	private ExistsDataProcessEnum getExistsDataProcess(Node<?> node) {
		String existDataProcessMode = null;
		if (node instanceof TableNode) {
			existDataProcessMode = ((TableNode) node).getExistDataProcessMode();
		} else if (node instanceof DatabaseNode) {
			existDataProcessMode = ((DatabaseNode) node).getExistDataProcessMode();
		}
		existDataProcessMode = StringUtils.isNotBlank(existDataProcessMode) ? existDataProcessMode : "keepData";
		return ExistsDataProcessEnum.fromOption(existDataProcessMode);
	}

	@Override
	void processEvents(List<TapEvent> tapEvents) {
		TapEvent foundDDLEvent = tapEvents.stream().filter(e -> e instanceof TapDDLEvent).findFirst().orElse(null);
		if (null == foundDDLEvent) {
			Map<String, List<TapEvent>> dmlEventsGroupByTableId = new HashMap<>();
			for (TapEvent tapEvent : tapEvents) {
				if (tapEvent instanceof TapRecordEvent) {
					String tableId = ((TapRecordEvent) tapEvent).getTableId();
					List<TapEvent> tapRecordEvents = dmlEventsGroupByTableId.computeIfAbsent(tableId, k -> new ArrayList<>());
					tapRecordEvents.add(tapEvent);
				}
			}
			dmlEventsGroupByTableId.forEach((tableId, tapRecordEvents)-> writeRecord(tapRecordEvents));
			if (obsLogger.isDebugEnabled()) {
				StringBuilder logStr = new StringBuilder("Target dispatch record events\n");
				dmlEventsGroupByTableId.forEach((k, v) -> logStr.append(" - ").append(k).append(": ").append(v.size()).append("\n"));
				obsLogger.debug(logStr.toString());
			}
		} else {
			dispatchTapRecordEvents(tapEvents,
					dispatchEntity -> {
						if (dispatchEntity.getCurrentTapEvent() instanceof TapRecordEvent && dispatchEntity.getLastTapEvent() instanceof TapRecordEvent) {
							return !((TapRecordEvent) dispatchEntity.getLastTapEvent()).getTableId().equals(((TapRecordEvent) dispatchEntity.getCurrentTapEvent()).getTableId());
						} else {
							return true;
						}
					},
					events -> {
						TapEvent firstEvent = events.get(0);
						if (firstEvent instanceof TapRecordEvent) {
							writeRecord(events);
						} else if (firstEvent instanceof TapDDLEvent) {
							writeDDL(events);
						}
					});
		}
	}

	private void writeDDL(List<TapEvent> events) {
		List<TapDDLEvent> tapDDLEvents = new ArrayList<>();
		events.forEach(event -> {
			TapDDLEvent tapDDLEvent = (TapDDLEvent) event;
			tapDDLEvent.setTableId(getTgtTableNameFromTapEvent(tapDDLEvent));
			tapDDLEvents.add(tapDDLEvent);
		});
		for (TapDDLEvent tapDDLEvent : tapDDLEvents) {
			Object result = ddlEventHandlers.handle(tapDDLEvent);
			if (null == result) {
				obsLogger.warn("DDL event: " + tapDDLEvent.getClass().getSimpleName() + " does not supported");
			}
		}
		uploadDagService.compareAndSet(false, true);
	}

	private boolean executeNewFieldFunction(TapNewFieldEvent tapNewFieldEvent) {
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tapNewFieldEvent.getTableId());
		if (null == tapTable) {
			throw new TapEventException(TaskTargetProcessorExCode_15.ADD_NEW_FIELD_GET_TAP_TABLE_FAILED, "Table id: " + tapNewFieldEvent.getTableId())
					.addEvent(tapNewFieldEvent);
		}
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isNotEmpty(nameFieldMap)) {
			List<TapField> newFields = tapNewFieldEvent.getNewFields();
			for (TapField newField : newFields) {
				String fieldName = newField.getName();
				TapField tapField = tapTable.getNameFieldMap().get(fieldName);
				if (null == tapField) {
					throw new TapEventException(TaskTargetProcessorExCode_15.ADD_NEW_FIELD_IS_NULL, "Table id: " + tapNewFieldEvent.getTableId() + ", field name: " + fieldName)
							.addEvent(tapNewFieldEvent);
				}
				newField.setDataType(tapTable.getNameFieldMap().get(fieldName).getDataType());
			}
		}
		ConnectorNode connectorNode = getConnectorNode();
		NewFieldFunction function = connectorNode.getConnectorFunctions().getNewFieldFunction();
		PDKMethod pdkMethod = PDKMethod.NEW_FIELD;
		if (null == function) {
			// only execute start function aspect so that it would be cheated as input
			AspectUtils.executeAspect(new NewFieldFuncAspect()
					.newFieldEvent(tapNewFieldEvent)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext).state(NewFieldFuncAspect.STATE_START));
			obsLogger.warn("PDK connector " + connectorNode.getConnectorContext().getSpecification().getId() + " does not support " + pdkMethod);
			return false;
		}
		try {
			executeDataFuncAspect(NewFieldFuncAspect.class, () -> new NewFieldFuncAspect()
					.newFieldEvent(tapNewFieldEvent)
					.connectorContext(connectorNode.getConnectorContext())
					.dataProcessorContext(dataProcessorContext)
					.start(), (newFieldFuncAspect ->
					PDKInvocationMonitor.invoke(connectorNode, pdkMethod,
							() -> function.newField(connectorNode.getConnectorContext(), tapNewFieldEvent), TAG)));
		} catch (Exception e) {
			Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.ADD_NEW_FIELD_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
						.addEvent(tapNewFieldEvent);
			}
		}
		return true;
	}

	private List<String> executeAlterFieldNameFunction(List<String> fields, ValueChange<String> nameChange) {
		if (null != fields) {
			int idx = fields.indexOf(nameChange.getBefore());
			if (-1 != idx) {
				fields.set(idx, nameChange.getAfter());
			}
		}
		return fields;
	}

	private boolean executeAlterFieldNameFunction(TapAlterFieldNameEvent tapAlterFieldNameEvent) {
		// 字段名变更
		Optional.ofNullable(tapAlterFieldNameEvent.getNameChange()
		).ifPresent(nameChange -> {
			// 修改关联字段配置
			updateConditionFieldsMap.computeIfPresent(tapAlterFieldNameEvent.getTableId(), (tableId, updateConditionFields) -> executeAlterFieldNameFunction(updateConditionFields, nameChange));

			// 修改自定义并发写入分区字段
			Optional.ofNullable(concurrentWritePartitionMap)
					.ifPresent(partitionFieldsMap-> partitionFieldsMap.computeIfPresent(tapAlterFieldNameEvent.getTableId(), (tableId, fields) -> executeAlterFieldNameFunction(fields, nameChange)));
		});

		ConnectorNode connectorNode = getConnectorNode();
		AlterFieldNameFunction function = connectorNode.getConnectorFunctions().getAlterFieldNameFunction();
		PDKMethod pdkMethod = PDKMethod.ALTER_FIELD_NAME;
		if (null == function) {
			// only execute start function aspect so that it would be cheated as input
			AspectUtils.executeAspect(new AlterFieldNameFuncAspect()
					.alterFieldNameEvent(tapAlterFieldNameEvent)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext).state(AlterFieldNameFuncAspect.STATE_START));
			obsLogger.warn("PDK connector " + connectorNode.getConnectorContext().getSpecification().getId() + " does not support " + pdkMethod);
			return false;
		}
		try {
			executeDataFuncAspect(AlterFieldNameFuncAspect.class, () -> new AlterFieldNameFuncAspect()
					.alterFieldNameEvent(tapAlterFieldNameEvent)
					.connectorContext(connectorNode.getConnectorContext())
					.dataProcessorContext(dataProcessorContext)
					.start(), (alterFieldNameFuncAspect) ->
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.ALTER_FIELD_NAME,
							() -> function.alterFieldName(connectorNode.getConnectorContext(), tapAlterFieldNameEvent),
							TAG));
		} catch (Exception e) {
			Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.ALTER_FIELD_NAME_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
						.addEvent(tapAlterFieldNameEvent);
			}
		}
		return true;
	}

	private boolean executeAlterFieldAttrFunction(TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tapAlterFieldAttributesEvent.getTableId());
		if (null == tapTable) {
			throw new TapEventException(TaskTargetProcessorExCode_15.ALTER_FIELD_ATTR_CANNOT_GET_TAP_TABLE, String.format("Table id: %s", tapAlterFieldAttributesEvent.getTableId()))
					.addEvent(tapAlterFieldAttributesEvent);
		}
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isNotEmpty(nameFieldMap)) {
			if (null != tapAlterFieldAttributesEvent.getDataTypeChange() && StringUtils.isNotBlank(tapAlterFieldAttributesEvent.getDataTypeChange().getAfter())) {
				String fieldName = tapAlterFieldAttributesEvent.getFieldName();
				String dataType = nameFieldMap.get(fieldName).getDataType();
				tapAlterFieldAttributesEvent.getDataTypeChange().setAfter(dataType);
			}
		}
		ConnectorNode connectorNode = getConnectorNode();
		AlterFieldAttributesFunction function = connectorNode.getConnectorFunctions().getAlterFieldAttributesFunction();
		PDKMethod pdkMethod = PDKMethod.ALTER_FIELD_ATTRIBUTES;
		if (null == function) {
			// only execute start function aspect so that it would be cheated as input
			AspectUtils.executeAspect(new AlterFieldAttributesFuncAspect()
					.alterFieldAttributesEvent(tapAlterFieldAttributesEvent)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext).state(AlterFieldAttributesFuncAspect.STATE_START));
			obsLogger.warn("PDK connector " + connectorNode.getConnectorContext().getSpecification().getId() + " does not support " + pdkMethod);
			return false;
		}
		try {
			executeDataFuncAspect(AlterFieldAttributesFuncAspect.class, () -> new AlterFieldAttributesFuncAspect()
					.alterFieldAttributesEvent(tapAlterFieldAttributesEvent)
					.connectorContext(connectorNode.getConnectorContext())
					.dataProcessorContext(dataProcessorContext)
					.start(), (alterFieldAttributesFuncAspect ->
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.ALTER_FIELD_ATTRIBUTES,
							() -> function.alterFieldAttributes(connectorNode.getConnectorContext(), tapAlterFieldAttributesEvent),
							TAG)));
		} catch (Exception e) {
			Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.ALTER_FIELD_ATTR_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
						.addEvent(tapAlterFieldAttributesEvent);
			}
		}
		return true;
	}

	private boolean executeDropFieldFunction(TapDropFieldEvent tapDropFieldEvent) {
		ConnectorNode connectorNode = getConnectorNode();
		DropFieldFunction function = connectorNode.getConnectorFunctions().getDropFieldFunction();
		PDKMethod pdkMethod = PDKMethod.DROP_FIELD;
		if (null == function) {
			// only execute start function aspect so that it would be cheated as input
			AspectUtils.executeAspect(new DropFieldFuncAspect()
					.dropFieldEvent(tapDropFieldEvent)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext).state(DropFieldFuncAspect.STATE_START));
			obsLogger.warn("PDK connector " + connectorNode.getConnectorContext().getSpecification().getId() + " does not support " + pdkMethod);
			return false;
		}
		try {
			executeDataFuncAspect(DropFieldFuncAspect.class, () -> new DropFieldFuncAspect()
					.dropFieldEvent(tapDropFieldEvent)
					.connectorContext(connectorNode.getConnectorContext())
					.dataProcessorContext(dataProcessorContext)
					.start(), (dropFieldFuncAspect ->
					PDKInvocationMonitor.invoke(connectorNode, PDKMethod.DROP_FIELD,
							() -> function.dropField(connectorNode.getConnectorContext(), tapDropFieldEvent),
							TAG)));
		} catch (Exception e) {
			Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.DROP_FIELD_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
						.addEvent(tapDropFieldEvent);
			}
		}
		return true;
	}

	private boolean executeCreateTableFunction(TapCreateTableEvent tapCreateTableEvent) {
		String tgtTableName = getTgtTableNameFromTapEvent(tapCreateTableEvent);
		TapTable tgtTapTable = dataProcessorContext.getTapTableMap().get(tgtTableName);
		return createTable(tgtTapTable, new AtomicBoolean());
	}

	private boolean executeCreateIndexFunction(TapCreateIndexEvent tapCreateIndexEvent) {
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		CreateIndexFunction createIndexFunction = getConnectorNode().getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			return true;
		}
		for (String tableId : tapTableMap.keySet()) {
			if (!isRunning()) {
				return true;
			}
			TapTable tapTable = tapTableMap.get(tableId);
			if (null == tapTable) {
				throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_TABLE_NOT_FOUND, String.format("Table id: %s", tableId))
						.addEvent(tapCreateIndexEvent);
			}

			try {
				executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
						.table(tapTable)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.createIndexEvent(tapCreateIndexEvent)
						.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
						PDKMethod.TARGET_CREATE_INDEX,
						() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, tapCreateIndexEvent), TAG));
			} catch (Exception e) {
				Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
				if (null != matched) {
					throw (TapCodeException) matched;
				}else {
					throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_EXECUTE_FAILED, String.format("Execute PDK method: %s", PDKMethod.TARGET_CREATE_INDEX), e)
							.addEvent(tapCreateIndexEvent);
				}
			}
		}
		return true;
	}

	private void writeRecord(List<TapEvent> events) {
		List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
		events.forEach(event -> tapRecordEvents.add((TapRecordEvent) event));
		TapRecordEvent firstEvent = tapRecordEvents.get(0);
		String tableId = firstEvent.getTableId();
		String tgtTableName = getTgtTableNameFromTapEvent(firstEvent);
		if (StringUtils.isBlank(tgtTableName)) {
			throw new TapEventException(TaskTargetProcessorExCode_15.WRITE_RECORD_GET_TARGET_TABLE_NAME_FAILED, String.format("Source table id: %s", tableId)).addEvent(firstEvent);
		}
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tgtTableName);
		handleTapTablePrimaryKeys(tapTable);
		events.forEach(this::addPropertyForMergeEvent);
		tapRecordEvents.forEach(t -> removeNotSupportFields(t, tapTable.getId()));
		WriteRecordFunction writeRecordFunction = getConnectorNode().getConnectorFunctions().getWriteRecordFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		if (writeRecordFunction != null) {
			logger.debug("Write {} of record events, {}", tapRecordEvents.size(), LoggerUtils.targetNodeMessage(getConnectorNode()));
			try {
				executeDataFuncAspect(WriteRecordFuncAspect.class, () -> new WriteRecordFuncAspect()
						.recordEvents(tapRecordEvents)
						.table(tapTable)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), (writeRecordFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_WRITE_RECORD,
								pdkMethodInvoker.runnable(
										() -> {
											ConnectorNode connectorNode = getConnectorNode();
											if (null == connectorNode) {
												throw new NodeException("Node is stopped, need to exit write_record").context(getDataProcessorContext());
											}

											Consumer<WriteListResult<TapRecordEvent>> resultConsumer = (writeListResult) -> {
												Map<TapRecordEvent, Throwable> errorMap = writeListResult.getErrorMap();
												if (MapUtils.isNotEmpty(errorMap)) {
													int recordWarnCounter = 1;
													for (Map.Entry<TapRecordEvent, Throwable> tapRecordEventThrowableEntry : errorMap.entrySet()) {
														String warnMsg = tapRecordEventThrowableEntry.getValue().getMessage() + "\n - Error record: " + tapRecordEventThrowableEntry.getKey()
																+ "\n - Stack trace: " + Log4jUtil.getStackString(tapRecordEventThrowableEntry.getValue());
														if (recordWarnCounter > MAX_RECORD_OBS_WARN) {
															logger.warn(warnMsg);
														} else {
															obsLogger.warn(warnMsg);
															recordWarnCounter++;
															if (recordWarnCounter == MAX_RECORD_OBS_WARN) {
																int theRemainingAmount = errorMap.size() - recordWarnCounter;
																if (theRemainingAmount > 0) {
																	obsLogger.warn("The remaining {} write error record will not be printed on the interface, please go to tapdata-agent.log to check.", theRemainingAmount);
																}
															}
														}
													}
												}

												if (writeRecordFuncAspect != null)
													AspectUtils.accept(writeRecordFuncAspect.state(WriteRecordFuncAspect.STATE_WRITING).getConsumers(), tapRecordEvents, writeListResult);
												if (logger.isDebugEnabled()) {
													logger.debug("Wrote {} of record events, {}", tapRecordEvents.size(), LoggerUtils.targetNodeMessage(getConnectorNode()));
												}
											};

											AspectUtils.executeAspect(SkipErrorDataAspect.class, () -> new SkipErrorDataAspect()
													.dataProcessorContext(dataProcessorContext)
													.tapTable(tapTable)
													.tapRecordEvents(tapRecordEvents)
													.pdkMethodInvoker(pdkMethodInvoker)
													.writeOneFunction((subTapRecordEvents) -> {
														writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), subTapRecordEvents, tapTable, resultConsumer);
														return null;
													}));
											if (!pdkMethodInvoker.isEnableSkipErrorEvent()) {
												writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), tapRecordEvents, tapTable, resultConsumer);
											}
										}
								)
						)));
			} finally {
				removePdkMethodInvoker(pdkMethodInvoker);
			}
		} else {
			throw new TapCodeException(TaskTargetProcessorExCode_15.WRITE_RECORD_PDK_NONSUPPORT, String.format("PDK connector id: %s", getConnectorNode().getConnectorContext().getSpecification().getId()));
		}
	}

	private void addPropertyForMergeEvent(TapEvent tapEvent) {
		if (null == tapEvent) return;
		Object info = tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY);
		if (!(info instanceof MergeInfo)) return;
		MergeInfo mergeInfo = (MergeInfo) info;
		MergeTableProperties currentProperty = mergeInfo.getCurrentProperty();
		if (null == currentProperty) return;
		if (null == currentProperty.getMergeType()) {
			currentProperty.setMergeType(MergeTableProperties.MergeType.valueOf(writeStrategy));
		}
		if (currentProperty.getMergeType() == MergeTableProperties.MergeType.appendWrite) return;
		final String tgtTableName = getTgtTableNameFromTapEvent(tapEvent);
		List<String> updateConditionFields = updateConditionFieldsMap.get(tgtTableName);
		if (CollectionUtils.isEmpty(updateConditionFields)) return;
		if (CollectionUtils.isEmpty(currentProperty.getJoinKeys())) {
			List<Map<String, String>> joinKeys = new ArrayList<>();
			for (String updateConditionField : updateConditionFields) {
				HashMap<String, String> joinKey = new HashMap<>();
				joinKey.put("source", updateConditionField);
				joinKey.put("target", updateConditionField);
				joinKeys.add(joinKey);
			}
			currentProperty.setJoinKeys(joinKeys);
		}
	}

	@Override
	void transactionBegin() {
		ConnectorNode connectorNode = getConnectorNode();
		if (null == connectorNode) {
			return;
		}
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		TransactionBeginFunction transactionBeginFunction = connectorFunctions.getTransactionBeginFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		try {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TRANSACTION_BEGIN,
					() -> pdkMethodInvoker.runnable(() -> transactionBeginFunction.begin(connectorNode.getConnectorContext())), TAG);
		} finally {
			removePdkMethodInvoker(pdkMethodInvoker);
		}
	}

	@Override
	void transactionCommit() {
		ConnectorNode connectorNode = getConnectorNode();
		if (null == connectorNode) {
			return;
		}
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		TransactionCommitFunction transactionCommitFunction = connectorFunctions.getTransactionCommitFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		try {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TRANSACTION_BEGIN,
					() -> pdkMethodInvoker.runnable(() -> transactionCommitFunction.commit(connectorNode.getConnectorContext())), TAG);
		} finally {
			removePdkMethodInvoker(pdkMethodInvoker);
		}
	}

	@Override
	void transactionRollback() {
		ConnectorNode connectorNode = getConnectorNode();
		if (null == connectorNode) {
			return;
		}
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		TransactionRollbackFunction transactionRollbackFunction = connectorFunctions.getTransactionRollbackFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		try {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TRANSACTION_BEGIN,
					() -> pdkMethodInvoker.runnable(() -> transactionRollbackFunction.rollback(connectorNode.getConnectorContext())), TAG);
		} finally {
			removePdkMethodInvoker(pdkMethodInvoker);
		}
	}

	@Override
	void processExactlyOnceWriteCache(List<TapdataEvent> tapdataEvents) {
		ConnectorNode connectorNode = getConnectorNode();
		if (null == connectorNode) {
			return;
		}
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		List<TapRecordEvent> tapRecordEvents = tapdataEvents.parallelStream().map(TapdataEvent::getExactlyOnceWriteCache)
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_WRITE_RECORD,
				pdkMethodInvoker.runnable(() -> writeRecordFunction.writeRecord(
						connectorNode.getConnectorContext(),
						tapRecordEvents,
						dataProcessorContext.getTapTableMap().get(ExactlyOnceUtil.EXACTLY_ONCE_CACHE_TABLE_NAME),
						result -> {
							Map<TapRecordEvent, Throwable> errorMap = result.getErrorMap();
							if (MapUtils.isNotEmpty(errorMap)) {
								Iterator<Map.Entry<TapRecordEvent, Throwable>> iterator = errorMap.entrySet().iterator();
								Map.Entry<TapRecordEvent, Throwable> next = iterator.next();
								throw new TapCodeException(TapExactlyOnceWriteExCode_22.WRITE_CACHE_FAILED, "First error cache record: " + next.getKey(), next.getValue());
							}
						})
				));
	}

	@Override
	boolean eventExactlyOnceWriteCheckExists(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return false;
		if (null == tapdataEvent.getExactlyOnceWriteCache()) return false;
		ConnectorNode connectorNode = getConnectorNode();
		if (null == connectorNode) {
			return false;
		}
		TapInsertRecordEvent exactlyOnceWriteCache = tapdataEvent.getExactlyOnceWriteCache();
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(ExactlyOnceUtil.EXACTLY_ONCE_CACHE_TABLE_NAME);
		Map<String, Object> filter = exactlyOnceWriteCache.getFilter(tapTable.primaryKeys());
		TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create().match(DataMap.create(filter));

		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		AtomicBoolean result = new AtomicBoolean(false);
		try {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
					pdkMethodInvoker.runnable(
							() -> queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, rs -> {
								if (null != rs.getError()) {
									throw new TapCodeException(TapExactlyOnceWriteExCode_22.CHECK_CACHE_FAILED, "Check cache failed by filter: " + tapAdvanceFilter, rs.getError());
								}
								result.set(CollectionUtils.isNotEmpty(rs.getResults()));
							})
					));
		} finally {
			removePdkMethodInvoker(pdkMethodInvoker);
		}
		return result.get();
	}

	@Override
	public void doClose() throws Exception {
		super.doClose();
	}

	@Override
	protected void updateDAG(TapdataEvent tapdataEvent) {
		final TapEvent tapEvent = tapdataEvent.getTapEvent();
		if (tapEvent instanceof TapAlterFieldNameEvent) {
			TapAlterFieldNameEvent tapAlterFieldNameEvent = (TapAlterFieldNameEvent) tapEvent;
			String tableName = (getNode() instanceof TableNode)
					? ((TableNode) getNode()).getTableName()
					: tapAlterFieldNameEvent.getTableId();

			// 更新任务配置
			Optional.ofNullable(dataProcessorContext.getTaskDto()
			).map(TaskDto::getDag
			).map(dag -> dag.getNode(getNode().getId())
			).ifPresent(node -> {
				if (node instanceof DataParentNode) {
					((DataParentNode<?>) node).setConcurrentWritePartitionMap(concurrentWritePartitionMap);
				}

				if (node instanceof DatabaseNode) {
					((DatabaseNode) node).setUpdateConditionFieldMap(updateConditionFieldsMap);
				} else if (node instanceof TableNode) {
					((TableNode) node).setUpdateConditionFields(updateConditionFieldsMap.get(tableName));
				}
			});
		}

	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.google.common.collect.Maps;
import com.tapdata.constant.ConnectorConstant;
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
import io.tapdata.entity.TapConstraintException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.constraint.TapCreateConstraintEvent;
import io.tapdata.entity.event.ddl.constraint.TapDropConstraintEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.*;
import io.tapdata.entity.simplify.pretty.ClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TapEventException;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.*;
import io.tapdata.flow.engine.V2.entity.SyncProgressNodeType;
import io.tapdata.flow.engine.V2.exactlyonce.ExactlyOnceUtil;
import io.tapdata.flow.engine.V2.exception.TapExactlyOnceWriteExCode_22;
import io.tapdata.flow.engine.V2.policy.PDkNodeInsertRecordPolicyService;
import io.tapdata.flow.engine.V2.policy.WritePolicyService;
import io.tapdata.flow.engine.V2.util.SyncTypeEnum;
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
import io.tapdata.pdk.core.error.TapPdkRunnerUnknownException;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeanUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
	public static final int MAX_INDEX_FIELDS_COUNT = 16;
	public static final int MAX_RECORD_OBS_WARN = 3;
	public static final int CREATE_INDEX_THRESHOLD = 5000000;
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkDataNode.class);
	private ClassHandlers ddlEventHandlers;
	private WritePolicyService writePolicyService;
	private Map<String, TapTable> partitionTapTables;
    @Getter
    private boolean writeGroupByTableEnable = true;
	private ConcurrentHashMap<String, PDKMethodInvoker> exactlyOncePdkMethodInvokerMap = new ConcurrentHashMap<>();

	public HazelcastTargetPdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		this.partitionTapTables = new ConcurrentHashMap<>();
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
                writeGroupByTableEnable = Boolean.TRUE.equals(dbNode.getWriteWithGroupByTableEnable());
			}
			if (getNode() instanceof DataParentNode) {
				writeStrategy = ((DataParentNode<?>) getNode()).getWriteStrategy();
			}
			initTargetDB();
			this.writePolicyService = new PDkNodeInsertRecordPolicyService(dataProcessorContext.getTaskDto(), getNode(), associateId);
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
		ddlEventHandlers.register(TapClearTableEvent.class,this::executeTruncateFunction);
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

    protected Set<String> filterSubPartitionTableTableMap() {
        TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
        if (syncTargetPartitionTableEnable) {
            //开启分区表时建表需要过滤掉子表
            return tapTableMap.keySet().stream().filter(name -> {
                TapTable tapTable = tapTableMap.get(name);
                return Objects.nonNull(tapTable) && !tapTable.checkIsSubPartitionTable();
            }).collect(Collectors.toSet());
        }
        return tapTableMap.keySet();
    }

	protected void initTargetDB() {
		obsLogger.info("Apply table structure to target database");
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		Set<String> tableIds = filterSubPartitionTableTableMap();
		executeDataFuncAspect(TableInitFuncAspect.class, () -> new TableInitFuncAspect()
				.tapTableMap(tapTableMap)
				.totals(tableIds.size())
				.dataProcessorContext(dataProcessorContext)
				.start(), (funcAspect -> {
			Node<?> node = dataProcessorContext.getNode();
			ExistsDataProcessEnum existsDataProcessEnum = getExistsDataProcess(node);
			Map<String, SyncProgress> allSyncProgress = foundAllSyncProgress(dataProcessorContext.getTaskDto().getAttrs());
			SyncProgress syncProgress = foundNodeSyncProgress(allSyncProgress, SyncProgressNodeType.TARGET);
			if (null == syncProgress) {
				Set<String> createdTableIds = new HashSet<>();
				for (String tableId : tableIds) {
					if (!isRunning()) {
						return;
					}
					boolean created = createTable(tapTableMap, funcAspect, node, existsDataProcessEnum, tableId,true);
					if (created) {
						createdTableIds.add(tableId);
					}
				}
				createForeignKeyConstraints(tapTableMap, createdTableIds);
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
								if (tableNameRelation != null && !tableNameRelation.isEmpty()) {
									sourceAndTargetMap = syncObjects1.getTableNameRelation();
								}
								Set<String> createdTableIds = new HashSet<>();
								for (String ldpNewTable : ldpNewTables) {
									String tableId = sourceAndTargetMap.get(ldpNewTable);
									if (tableId != null) {
										if (!isRunning()) {
											return;
										}
										boolean created = createTable(tapTableMap, funcAspect, node, existsDataProcessEnum, tableId,true);
										if (created) {
											createdTableIds.add(tableId);
										}
									}
								}
								createForeignKeyConstraints(tapTableMap, createdTableIds);
							}
						}
					}
				}
			}
		}));
	}

	protected boolean createTable(TapTableMap<String, TapTable> tapTableMap, TableInitFuncAspect funcAspect, Node<?> node, ExistsDataProcessEnum existsDataProcessEnum, String tableId,boolean init) {
		TapTable tapTable = tapTableMap.get(tableId);
		List<String> updateConditionFields = getUpdateConditionFields(node, tapTable);
		if (null == tapTable) {
			TapCodeException e = new TapCodeException(TaskTargetProcessorExCode_15.INIT_TARGET_TABLE_TAP_TABLE_NULL, "Table name: " + tableId).dynamicDescriptionParameters(tableId);
			if (null != funcAspect) funcAspect.setThrowable(e);
			throw e;
		}
		if (StringUtils.isNotBlank(tableId) && StringUtils.equalsAny(tableId, ExactlyOnceUtil.EXACTLY_ONCE_CACHE_TABLE_NAME)) {
			return false;
		}
		dropTable(existsDataProcessEnum, tapTable, init);
		AtomicBoolean succeed = new AtomicBoolean(false);
		List<TapIndex> indexList = tapTable.getIndexList();
		boolean createdTable = createTable(tapTable, succeed, init);
		clearData(existsDataProcessEnum, tableId);
		//sync index
		syncIndex(tableId, tapTable, indexList, succeed.get());
		createTargetIndex(updateConditionFields, succeed.get(), tableId, tapTable, createdTable, indexList);
		if (null != funcAspect)
			funcAspect.state(TableInitFuncAspect.STATE_PROCESS).completed(tableId, createdTable);
		return createdTable;
	}

	protected void createForeignKeyConstraints(TapTableMap<String, TapTable> tapTableMap, Set<String> tableIds) {
		if (!checkSyncForeignKeyOpen()) return;
		TaskDto taskDto = dataProcessorContext.getTaskDto();
		if (!StringUtils.equalsAnyIgnoreCase(taskDto.getSyncType(), TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC) || !taskDto.isNormalTask()) {
			return;
		}
		Optional.ofNullable(getConnectorNode()).ifPresent(connectorNode -> Optional.ofNullable(connectorNode.getConnectorFunctions()).ifPresent(connectorFunctions -> {
			QueryConstraintsFunction queryConstraintsFunction = connectorFunctions.getQueryConstraintsFunction();
			CreateConstraintFunction createConstraintFunction = connectorFunctions.getCreateConstraintFunction();
			if (null == queryConstraintsFunction || null == createConstraintFunction) {
				return;
			}
			List<String> saveForeignKeys = new ArrayList<>();
			for (String tableId : tableIds) {
				if (!isRunning()) {
					break;
				}
				TapTable tapTable = tapTableMap.get(tableId);
				List<TapConstraint> constraintList = tapTable.getConstraintList();
				if (CollectionUtils.isEmpty(constraintList)) {
					continue;
				}
				List<TapConstraint> tobeCreateForeignKeys = new ArrayList<>();
				List<TapConstraint> tobeDropForeignKeys = new ArrayList<>();
				try {
					queryConstraintsFunction.query(connectorNode.getConnectorContext(), tapTable, existsConstraints -> {
						for (TapConstraint tapConstraint : constraintList) {
							if (!TapConstraint.ConstraintType.FOREIGN_KEY.equals(tapConstraint.getType())) {
								continue;
							}
							TapConstraint existsForeignKey = existsConstraints.stream().filter(ec -> {
								if (!ec.getType().equals(tapConstraint.getType())) {
									return false;
								}
								if (!ec.getReferencesTableName().equals(tapConstraint.getReferencesTableName())) {
									return false;
								}
								List<TapConstraintMapping> existsMappingFields = ec.getMappingFields();
								List<TapConstraintMapping> mappingFields = tapConstraint.getMappingFields();
								if (null == existsMappingFields || null == mappingFields) {
									return false;
								}
								String existsForeignKeyString = existsMappingFields.stream().map(TapConstraintMapping::getForeignKey).sorted().collect(Collectors.joining(","));
								String foreignKeyString = mappingFields.stream().map(TapConstraintMapping::getForeignKey).sorted().collect(Collectors.joining(","));
								if (!existsForeignKeyString.equals(foreignKeyString)) {
									return false;
								}
								String existsReferenceKeyString = existsMappingFields.stream().map(TapConstraintMapping::getReferenceKey).sorted().collect(Collectors.joining(","));
								String referenceKeyString = mappingFields.stream().map(TapConstraintMapping::getReferenceKey).sorted().collect(Collectors.joining(","));
								if (!existsReferenceKeyString.equals(referenceKeyString)) {
									return false;
								}
								return true;
							}).findFirst().orElse(null);
							if (null != existsForeignKey) {
								tobeDropForeignKeys.add(existsForeignKey);
								continue;
							}
							tobeCreateForeignKeys.add(tapConstraint);
						}
					});
				} catch (Throwable e) {
					obsLogger.warn("Before creating a foreign key, check whether the foreign key already exists, the query fails. Table name: {}, the error will be ignored, please check and create by manually", tableId, e);
				}
				if(connectorNode.getConnectorContext().getSpecification().getTags().contains("disableForeignKey")){
					createForeignKeyConstraints(tobeCreateForeignKeys,tapTable,createConstraintFunction);
				}else{
					List<String> dropForeignKeys = dropForeignKeyConstraints(tobeCreateForeignKeys,tobeDropForeignKeys,connectorNode,tapTable,createConstraintFunction,connectorFunctions.getDropConstraintFunction());
					if(CollectionUtils.isNotEmpty(dropForeignKeys)) saveForeignKeys.addAll(dropForeignKeys);
				}
			}
			if(CollectionUtils.isNotEmpty(saveForeignKeys))saveForeignKeySql(saveForeignKeys);
		}));
	}

	protected void createForeignKeyConstraints(List<TapConstraint> tobeCreateForeignKeys,TapTable tapTable,CreateConstraintFunction createConstraintFunction){
		try {
			TapCreateConstraintEvent tapCreateConstraintEvent = new TapCreateConstraintEvent();
			tapCreateConstraintEvent.constraintList(tobeCreateForeignKeys);
			createConstraintFunction.createConstraint(getConnectorNode().getConnectorContext(), tapTable, tapCreateConstraintEvent, true);
		} catch (Throwable e) {
			if (e instanceof TapConstraintException) {
				TapConstraintException tapConstraintException = (TapConstraintException) e;
				List<String> sqlList = tapConstraintException.getSqlList();
				List<Throwable> exceptions = tapConstraintException.getExceptions();
				if (CollectionUtils.isNotEmpty(sqlList)) {
					for (int i = 0; i < sqlList.size(); i++) {
						String sql = sqlList.get(i);
						Throwable cause = exceptions.get(i);
						if (null == cause) {
							obsLogger.warn("Failed to create a foreign key, table name: {}, sql: {}", tapTable.getId(), sql);
						} else {
							obsLogger.warn("Failed to create a foreign key, table name: {}, sql: {}, error: {}", tapTable.getId(), sql, Log4jUtil.getStackString(cause));
						}
					}
				}
			} else {
				obsLogger.warn("Due to unknown error, the creation of foreign key failed, the table name: {}, this step will be skipped, please check manually and create", tapTable.getId(), e);
			}

		}
	}

	protected List<String> dropForeignKeyConstraints(List<TapConstraint> tobeCreateForeignKeys,List<TapConstraint> tobeDropForeignKeys,ConnectorNode connectorNode,TapTable tapTable,CreateConstraintFunction createConstraintFunction,DropConstraintFunction dropConstraintFunction){
		if(null == dropConstraintFunction) return null;
		TapDropConstraintEvent tapDropConstraintEvent = new TapDropConstraintEvent();
		TapCreateConstraintEvent tapCreateConstraintEvent = new TapCreateConstraintEvent();
		tapDropConstraintEvent.setConstraintList(tobeDropForeignKeys);
		tobeCreateForeignKeys.addAll(tobeDropForeignKeys);
		tapCreateConstraintEvent.setConstraintList(tobeCreateForeignKeys);
		try{
			dropConstraintFunction.dropConstraint(connectorNode.getConnectorContext(), tapTable, tapDropConstraintEvent);
		}catch (Throwable e){
			obsLogger.warn("Failed to delete the foreign key: {}", e.getMessage());
		}
		try {
			createConstraintFunction.createConstraint(connectorNode.getConnectorContext(), tapTable, tapCreateConstraintEvent, false);
		}catch (Throwable e){
			obsLogger.warn("Failed to get foreign key sql ,table name: {}, error: {}", tapTable.getId(), e);
		}
		return tapCreateConstraintEvent.getConstraintSqlList();

	}

	protected void saveForeignKeySql(List<String> sqlList) {
		try {
			if(CollectionUtils.isNotEmpty(sqlList)){
				Map<String, Object> queryMap = new HashMap<>();
				queryMap.put("taskId",dataProcessorContext.getTaskDto().getId().toHexString());
				Map<String, Object> updateMap = new HashMap<>();
				updateMap.put("sqlList",sqlList);
				clientMongoOperator.upsert(queryMap, updateMap, ConnectorConstant.FOREIGN_KEY_CONSTRAINT);
			}
		}catch (Throwable e){
			obsLogger.warn("Failed to get foreign key sql ,taskId: {}, error: {}",dataProcessorContext.getTaskDto().getId().toHexString(), e);
		}
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

	protected void createTargetIndex(List<String> updateConditionFields, boolean createUnique, String tableId, TapTable tapTable, boolean createdTable, List<TapIndex> indexList) {

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
			if(unwindProcess) createUnique = false;
			if (!checkCreateUniqueIndexOpen()) createUnique = false;
			TapIndex tapIndex = new TapIndex().unique(createUnique);
			List<TapIndexField> tapIndexFields = new ArrayList<>();
			if (null == updateConditionFields) {
				obsLogger.warn("Table " + tableId + " index fields is null, will not create index automatically");
				return;
			}
			if (updateConditionFields.size() > MAX_INDEX_FIELDS_COUNT) {
				obsLogger.warn("Table " + tableId + " index field exceeds the maximum value of 16, the index will not be created automatically, please create it manually");
				return;
			}
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				boolean usePkAsUpdateConditions = usePkAsUpdateConditions(updateConditionFields, tapTable.primaryKeys());
				if (usePkAsUpdateConditions && createdTable) {
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

				List<TapIndex> existsIndexes = queryExistsIndexes(tapTable, tapIndices);
				if(CollectionUtils.isNotEmpty(existsIndexes)){
					existsIndexes.forEach(i -> obsLogger.trace("Table: {} already exists Index: {} and will no longer create index", tableId, i));
					if (existsIndexes.size() == tapIndices.size()) return;
				}
				executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
						.table(tapTable)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.createIndexEvent(indexEvent.get())
						.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
						PDKMethod.TARGET_CREATE_INDEX,
						() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, indexEvent.get()), TAG, buildErrorConsumer(tableId)));
			}
		} catch (Throwable throwable) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED, "Table name: " + tableId, throwable)
					.addEvent(indexEvent.get())
					.dynamicDescriptionParameters(tableId, indexEvent.get().getIndexList());
			throwTapCodeException(throwable,tapEventException);
		}
	}
	protected boolean checkCreateUniqueIndexOpen(){
		Node node = getNode();
		if (node instanceof DatabaseNode || node instanceof TableNode) {
			DataParentNode dataParentNode = (DataParentNode) node;
			return !Boolean.FALSE.equals(dataParentNode.getUniqueIndexEnable());
		}
		return true;
	}

	protected void syncIndex(String tableId, TapTable tapTable, List<TapIndex> tapIndexList, boolean autoCreateTable) throws TapEventException {
		long start = System.currentTimeMillis();
		if (!checkSyncIndexOpen()) return;
		if (!autoCreateTable) {
			obsLogger.warn("Table: {} already exists and will no longer synchronize indexes", tableId);
			return;
		}
		CreateIndexFunction createIndexFunction = getConnectorNode().getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			obsLogger.warn("Target connector does not support create index and will no longer synchronize indexes");
			return;
		}
		GetTableInfoFunction getTableInfoFunction = getConnectorNode().getConnectorFunctions().getGetTableInfoFunction();
		if (null == getTableInfoFunction) {
			obsLogger.warn("Target connector does not support get table information and will no longer synchronize indexes");
			return;
		}
		QueryIndexesFunction queryIndexesFunction = getConnectorNode().getConnectorFunctions().getQueryIndexesFunction();
		if (null == queryIndexesFunction) {
			obsLogger.warn("Target connector does not support query index and will no longer synchronize indexes");
			return;
		}
		AtomicReference<TapCreateIndexEvent> indexEvent = new AtomicReference<>();
		try {
			//query table info
			TableInfo tableInfo = getTableInfoFunction.getTableInfo(getConnectorNode().getConnectorContext(), tableId);
			if (null != tableInfo) {
				if (null == tableInfo.getNumOfRows()) {
					obsLogger.warn("Table: {} records amount is unknown and will no longer synchronize indexes", tableId);
					return;
				}
				if (tableInfo.getNumOfRows() > CREATE_INDEX_THRESHOLD) {
					obsLogger.warn("Table: {} records amount exceeds the threshold: {} for creating indexes and will no longer synchronize indexes", tableId, CREATE_INDEX_THRESHOLD);
					return;
				}
			} else {
				obsLogger.warn("Table: {} gets table information failed and will no longer synchronize indexes", tableId);
				return;
			}
			List<TapIndex> indexList = new ArrayList<>();
			List<TapIndex> indices = tapIndexList;
			if (null == indices) {
				indices = new ArrayList<>();
			}
			indices.forEach(index -> {
				TapIndex tapIndex = new TapIndex().unique(index.getUnique()).primary(index.getPrimary());
				tapIndex.setIndexFields(index.getIndexFields());
				tapIndex.setName(index.getName());
				tapIndex.setCluster(index.getCluster());
				indexList.add(tapIndex);
			});
			List<TapIndex> existsIndexes = queryExistsIndexes(tapTable, indexList);
			if (CollectionUtils.isNotEmpty(existsIndexes)) {
				existsIndexes.forEach(i -> {
					obsLogger.trace("Table: {} already exists Index: {} and will no longer create index", tableId, i.getName());
					indexList.remove(i);
				});
			}
			if (CollectionUtils.isEmpty(indexList)) {
				obsLogger.trace("Table: {} already exists Index list: {}", tableId, indices);
				return;
			}
			indexList.forEach(index->{
				long currentIndexStart = System.currentTimeMillis();
				indexEvent.set(createIndexEvent(tableId, Collections.singletonList(index)));
				obsLogger.trace("Table: {} will create Index: {}", indexEvent.get().getTableId(), index);
				executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
						.table(tapTable)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.createIndexEvent(indexEvent.get())
						.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
						PDKMethod.TARGET_CREATE_INDEX,
						() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, indexEvent.get()), TAG, buildErrorConsumer(tableId)));
				long currentIndexEnd = System.currentTimeMillis();
				obsLogger.trace("Table: {} create Index: {} successfully, cost {}ms", indexEvent.get().getTableId(), index.getName(), currentIndexEnd - currentIndexStart);
			});
		} catch (Throwable throwable) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED, "Table name: " + tableId, throwable)
					.addEvent(indexEvent.get())
					.dynamicDescriptionParameters(tableId, indexEvent.get() != null ? indexEvent.get().getIndexList() : null);
			throwTapCodeException(throwable,tapEventException);
		}
		long end = System.currentTimeMillis();
		obsLogger.trace("Table: {} synchronize indexes completed, cost {}ms totally", tableId, end - start);
	}

	protected List<TapIndex> queryExistsIndexes(TapTable tapTable, List<TapIndex> indexList) throws Throwable {
		QueryIndexesFunction queryIndexesFunction = getConnectorNode().getConnectorFunctions().getQueryIndexesFunction();
		if (null == queryIndexesFunction) {
			return new ArrayList<>();
		}
		List<TapIndex> existsIndexes = new ArrayList<>();
		queryIndexesFunction.query(getConnectorNode().getConnectorContext(), tapTable, (tapIndexList)-> tapIndexList.forEach(existsIndex -> {
			// If the index already exists, it will no longer be created; Having the same name is considered as existence; Fields with the same order are also considered to exist
			for (TapIndex tapIndex : indexList) {
				if (tapIndexEquals(existsIndex, tapIndex, true)) {
					existsIndexes.add(tapIndex);
				}
			}
		}));
		return existsIndexes;
	}

	protected boolean tapIndexEquals(TapIndex index1, TapIndex index2, boolean compareIndexName) {
		if(null == index1 || null == index2) {
			return false;
		}
		if (compareIndexName) {
			String name1 = index1.getName();
			String name2 = index2.getName();
			if (null != name1 && name1.equals(name2)) {
				return true;
			}
		}
		List<TapIndexField> indexFields1 = index1.getIndexFields();
		List<TapIndexField> indexFields2 = index2.getIndexFields();
		if (indexFields1.size() != indexFields2.size()) {
			return false;
		}
		for (int i = 0; i < indexFields1.size(); i++) {
			TapIndexField tapIndexField1 = indexFields1.get(i);
			TapIndexField tapIndexField2 = indexFields2.get(i);
			if (!Objects.equals(tapIndexField1.getName(), tapIndexField2.getName())) {
				return false;
			}
			if (!Objects.equals(tapIndexField1.getFieldAsc(), tapIndexField2.getFieldAsc())) {
				return false;
			}
		}
		return true;
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

	protected boolean checkSyncForeignKeyOpen(){
		Node node = getNode();
		if (node instanceof DatabaseNode || node instanceof TableNode) {
			DataParentNode dataParentNode = (DataParentNode) node;
			if (Boolean.TRUE.equals(dataParentNode.getSyncForeignKeyEnable())) {
				return true;
			}
		}
		return false;
	}

	protected void clearData(ExistsDataProcessEnum existsDataProcessEnum, String tableId) {
		if (SyncTypeEnum.CDC == syncType || existsDataProcessEnum != ExistsDataProcessEnum.REMOVE_DATE) return;

		AtomicReference<TapClearTableEvent> tapClearTableEvent = new AtomicReference<>();
		try {
			ClearTableFunction clearTableFunction = getConnectorNode().getConnectorFunctions().getClearTableFunction();
			Optional.ofNullable(clearTableFunction).ifPresent(func -> {
				tapClearTableEvent.set(clearTableEvent(tableId));
				if (null != sourceConnection) {
					tapClearTableEvent.get().database(sourceConnection.getDatabase_name());
					tapClearTableEvent.get().schema(sourceConnection.getDatabase_owner());
				}
				executeDataFuncAspect(ClearTableFuncAspect.class, () -> new ClearTableFuncAspect()
						.clearTableEvent(tapClearTableEvent.get())
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), clearTableFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_CLEAR_TABLE, () -> func.clearTable(getConnectorNode().getConnectorContext(), tapClearTableEvent.get()), TAG, buildErrorConsumer(tapClearTableEvent.get().getTableId())));
			});
		} catch (Throwable throwable) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.CLEAR_TABLE_FAILED, "Table name: " + tableId, throwable)
					.addEvent(tapClearTableEvent.get())
					.dynamicDescriptionParameters(tableId);
			throwTapCodeException(throwable,tapEventException);
		}
	}

	protected void dropTable(ExistsDataProcessEnum existsDataProcessEnum, TapTable table, boolean init) {
		if (SyncTypeEnum.CDC == syncType || existsDataProcessEnum != ExistsDataProcessEnum.DROP_TABLE) return;

		AtomicReference<TapDropTableEvent> tapDropTableEvent = new AtomicReference<>();
		final String tableId = table.getId();
		try {
			DropTableFunction dropTableFunction = getConnectorNode().getConnectorFunctions().getDropTableFunction();
			DropPartitionTableFunction dropPartitionTableFunction = getConnectorNode().getConnectorFunctions().getDropPartitionTableFunction();
			final boolean needDropPartitionTable = syncTargetPartitionTableEnable
					&& Objects.nonNull(table.getPartitionInfo())
					&& Objects.nonNull(dropPartitionTableFunction);
			tapDropTableEvent.set(dropTableEvent(tableId));
			if (null != sourceConnection) {
				tapDropTableEvent.get().database(sourceConnection.getDatabase_name());
				tapDropTableEvent.get().schema(sourceConnection.getDatabase_owner());
			}
			masterTableId(tapDropTableEvent.get(), table);
			if (needDropPartitionTable) {
				executeDataFuncAspect(DropTableFuncAspect.class, () -> new DropTableFuncAspect()
						.setInit(init)
						.dropTableEvent(tapDropTableEvent.get())
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), (dropTableFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.DROP_PARTITION_TABLE_FUNCTION, () ->
									dropPartitionTableFunction.dropTable(
										getConnectorNode().getConnectorContext(),
										tapDropTableEvent.get()),
										TAG,
										buildErrorConsumer(tapDropTableEvent.get().getTableId())))
				);
			} else if (dropTableFunction != null) {
				executeDataFuncAspect(DropTableFuncAspect.class, () -> new DropTableFuncAspect()
						.setInit(init)
						.dropTableEvent(tapDropTableEvent.get())
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), (dropTableFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_DROP_TABLE, () -> dropTableFunction.dropTable(getConnectorNode().getConnectorContext(), tapDropTableEvent.get()), TAG, buildErrorConsumer(tapDropTableEvent.get().getTableId()))));
			} else {
				// only execute start function aspect so that it would be cheated as input
				AspectUtils.executeAspect(new DropTableFuncAspect()
						.dropTableEvent(tapDropTableEvent.get())
						.setInit(init)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext).state(NewFieldFuncAspect.STATE_START));
			}
		} catch (Throwable throwable) {
			TapCodeException tapCodeException = new TapEventException(TaskTargetProcessorExCode_15.DROP_TABLE_FAILED, "Table name: " + tableId, throwable)
					.addEvent(tapDropTableEvent.get())
					.dynamicDescriptionParameters(tableId);
			throwTapCodeException(throwable,tapCodeException);
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
		if (null == foundDDLEvent && isWriteGroupByTableEnable()) {
			Map<String, List<TapEvent>> dmlEventsGroupByTableId = new HashMap<>();
			for (TapEvent tapEvent : tapEvents) {
				if (tapEvent instanceof TapRecordEvent) {
					String tableId = getTgtTableNameFromTapEvent(tapEvent);
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

	protected void writeDDL(List<TapEvent> events) {
		List<TapDDLEvent> tapDDLEvents = new ArrayList<>();
		events.forEach(event -> {
			if (event instanceof TapCreateTableEvent) {
				TapCreateTableEvent createTableEvent = (TapCreateTableEvent) event;
				// 当前节点未启用分区表，忽略创建分区子表事件
				if (!syncTargetPartitionTableEnable
						&& createTableEvent.getTable().checkIsSubPartitionTable()) return;
			}
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

	protected boolean executeNewFieldFunction(TapNewFieldEvent tapNewFieldEvent) {
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tapNewFieldEvent.getTableId());
		if (null == tapTable) {
			throw new TapEventException(TaskTargetProcessorExCode_15.ADD_NEW_FIELD_GET_TAP_TABLE_FAILED, "Table id: " + tapNewFieldEvent.getTableId())
					.addEvent(tapNewFieldEvent)
					.dynamicDescriptionParameters(tapNewFieldEvent.getTableId());
		}
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		if (MapUtils.isNotEmpty(nameFieldMap)) {
			List<TapField> newFields = tapNewFieldEvent.getNewFields();
			for (TapField newField : newFields) {
				String fieldName = newField.getName();
				TapField tapField = tapTable.getNameFieldMap().get(fieldName);
				if (null == tapField) {
					throw new TapEventException(TaskTargetProcessorExCode_15.ADD_NEW_FIELD_IS_NULL, "Table id: " + tapNewFieldEvent.getTableId() + ", field name: " + fieldName)
							.addEvent(tapNewFieldEvent)
							.dynamicDescriptionParameters(tapNewFieldEvent.getTableId(),fieldName);
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
							() -> function.newField(connectorNode.getConnectorContext(), tapNewFieldEvent), TAG, buildErrorConsumer(tapTable.getId()))));
		} catch (Exception e) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.ADD_NEW_FIELD_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
					.addEvent(tapNewFieldEvent);
			throwTapCodeException(e,tapEventException);
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

	protected boolean executeAlterFieldNameFunction(TapAlterFieldNameEvent tapAlterFieldNameEvent) {
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
							TAG, buildErrorConsumer(tapAlterFieldNameEvent.getTableId())));
		} catch (Exception e) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.ALTER_FIELD_NAME_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
					.addEvent(tapAlterFieldNameEvent)
					.dynamicDescriptionParameters(tapAlterFieldNameEvent.getNameChange());
			throwTapCodeException(e,tapEventException);
		}
		return true;
	}

	protected boolean executeAlterFieldAttrFunction(TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tapAlterFieldAttributesEvent.getTableId());
		if (null == tapTable) {
			throw new TapEventException(TaskTargetProcessorExCode_15.ALTER_FIELD_ATTR_CANNOT_GET_TAP_TABLE, String.format("Table id: %s", tapAlterFieldAttributesEvent.getTableId()))
					.addEvent(tapAlterFieldAttributesEvent)
					.dynamicDescriptionParameters(tapAlterFieldAttributesEvent.getTableId());
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
							TAG, buildErrorConsumer(tapTable.getId()))));
		} catch (Exception e) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.ALTER_FIELD_ATTR_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
					.addEvent(tapAlterFieldAttributesEvent)
					.dynamicDescriptionParameters(tapAlterFieldAttributesEvent.getTableId(),tapAlterFieldAttributesEvent.getFieldName());
			throwTapCodeException(e,tapEventException);
		}
		return true;
	}

	protected boolean executeDropFieldFunction(TapDropFieldEvent tapDropFieldEvent) {
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
							TAG, buildErrorConsumer(tapDropFieldEvent.getTableId()))));
		} catch (Exception e) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.DROP_FIELD_EXECUTE_FAILED, String.format("Execute PDK method: %s", pdkMethod), e)
					.addEvent(tapDropFieldEvent)
					.dynamicDescriptionParameters(tapDropFieldEvent.getTableId(),tapDropFieldEvent.getFieldName());
			throwTapCodeException(e,tapEventException);
		}
		return true;
	}

	private boolean executeCreateTableFunction(TapCreateTableEvent tapCreateTableEvent) {
		String tgtTableName = getTgtTableNameFromTapEvent(tapCreateTableEvent);
		TapTable tgtTapTable = dataProcessorContext.getTapTableMap().get(tgtTableName);
		return createTable(tgtTapTable, new AtomicBoolean(),false);
	}

	protected boolean executeCreateIndexFunction(TapCreateIndexEvent tapCreateIndexEvent) {
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		String tableId = tapCreateIndexEvent.getTableId();
		if (StringUtils.isBlank(tableId)) {
			throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_EVENT_TABLE_ID_EMPTY).addEvent(tapCreateIndexEvent);
		}
		TapTable tapTable = tapTableMap.get(tableId);
		if (null == tapTable) {
			throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_TABLE_NOT_FOUND, String.format("Table id: %s", tableId))
					.addEvent(tapCreateIndexEvent)
					.dynamicDescriptionParameters(tableId);
		}
		CreateIndexFunction createIndexFunction = getConnectorNode().getConnectorFunctions().getCreateIndexFunction();
		if (null == createIndexFunction) {
			return false;
		}
		List<TapIndex> existsIndexes;
		try {
			existsIndexes = queryExistsIndexes(tapTable, tapCreateIndexEvent.getIndexList());
		} catch (Throwable e) {
			throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_QUERY_EXISTS_INDEX_FAILED, e)
					.addEvent(tapCreateIndexEvent);
		}
		if (CollectionUtils.isNotEmpty(existsIndexes)) {
			existsIndexes.forEach(tapCreateIndexEvent.getIndexList()::remove);
		}

		try {
			executeDataFuncAspect(CreateIndexFuncAspect.class, () -> new CreateIndexFuncAspect()
					.table(tapTable)
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(dataProcessorContext)
					.createIndexEvent(tapCreateIndexEvent)
					.start(), createIndexFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(),
					PDKMethod.TARGET_CREATE_INDEX,
					() -> createIndexFunction.createIndex(getConnectorNode().getConnectorContext(), tapTable, tapCreateIndexEvent), TAG, buildErrorConsumer(tableId)));
		} catch (Exception e) {
			Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
			if (null != matched) {
				throw (TapCodeException) matched;
			}else {
				throw new TapEventException(TaskTargetProcessorExCode_15.CREATE_INDEX_FAILED, String.format("Execute PDK method: %s", PDKMethod.TARGET_CREATE_INDEX), e)
						.addEvent(tapCreateIndexEvent)
						.dynamicDescriptionParameters(tableId,tapCreateIndexEvent.getIndexList());
			}
		}
		return true;
	}
	protected boolean executeTruncateFunction(TapClearTableEvent tapClearTableEvent) {
		try {
			ClearTableFunction clearTableFunction = getConnectorNode().getConnectorFunctions().getClearTableFunction();
			Optional.ofNullable(clearTableFunction).ifPresent(func -> {
				executeDataFuncAspect(TruncateTableFuncAspect.class, () -> new TruncateTableFuncAspect()
						.truncateTableEvent(tapClearTableEvent)
						.connectorContext(getConnectorNode().getConnectorContext())
						.dataProcessorContext(dataProcessorContext)
						.start(), truncateTableFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_CLEAR_TABLE, () -> func.clearTable(getConnectorNode().getConnectorContext(), tapClearTableEvent), TAG, buildErrorConsumer(tapClearTableEvent.getTableId())));
			});
		} catch (Throwable throwable) {
			TapCodeException tapEventException = new TapEventException(TaskTargetProcessorExCode_15.CLEAR_TABLE_FAILED, "Table name: " + tapClearTableEvent.getTableId(), throwable)
					.addEvent(tapClearTableEvent)
					.dynamicDescriptionParameters(tapClearTableEvent.getTableId());
			throwTapCodeException(throwable,tapEventException);
		}
		return true;
	}

	protected void writeRecord(List<TapEvent> events) {
		List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
		events.forEach(event -> tapRecordEvents.add((TapRecordEvent) event));
		TapRecordEvent firstEvent = tapRecordEvents.get(0);
		String tableId = firstEvent.getTableId();
		String tgtTableName = getTgtTableNameFromTapEvent(firstEvent);
		if (StringUtils.isBlank(tgtTableName)) {
			throw new TapEventException(TaskTargetProcessorExCode_15.WRITE_RECORD_GET_TARGET_TABLE_NAME_FAILED, String.format("Source table id: %s", tableId)).addEvent(firstEvent).dynamicDescriptionParameters(tableId);
		}
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tgtTableName);
		handleTapTablePrimaryKeys(tapTable);
		events.forEach(this::addPropertyForMergeEvent);

		tapRecordEvents.forEach(t -> {
			removeNotSupportFields(t, tapTable.getId());
		});
		WriteRecordFunction writeRecordFunction = getConnectorNode().getConnectorFunctions().getWriteRecordFunction();
		PDKMethodInvoker pdkMethodInvoker = createPdkMethodInvoker();
		if (writeRecordFunction != null) {
			logger.debug("Write {} of record events, {}", tapRecordEvents.size(), LoggerUtils.targetNodeMessage(getConnectorNode()));
			try {
				executeDataFuncAspect(WriteRecordFuncAspect.class, () -> {

					TapTable tapTableForObs = tapTable;
					if (firstEvent.getPartitionMasterTableId() != null) {
						tapTableForObs = partitionTapTables.computeIfAbsent(firstEvent.getPartitionMasterTableId(), key -> {
							TapTable cloneObj = new TapTable();
							BeanUtils.copyProperties(tapTable, cloneObj);
							cloneObj.setId(key);
							TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
							String ancestorsName = tapTableMap.containsKey(firstEvent.getPartitionMasterTableId()) ?
									tapTableMap.get(firstEvent.getPartitionMasterTableId()).getAncestorsName() : null;
							if (ancestorsName == null)
								ancestorsName = tapTable.getAncestorsName() != null ? tapTable.getAncestorsName() : key;
							cloneObj.setName(ancestorsName);
							return cloneObj;
						});
					}

					return new WriteRecordFuncAspect()
							.recordEvents(tapRecordEvents)
							.table(tapTableForObs)
							.connectorContext(getConnectorNode().getConnectorContext())
							.dataProcessorContext(dataProcessorContext)
							.start();
				}, writeRecordFuncAspect ->
						PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.TARGET_WRITE_RECORD,
								pdkMethodInvoker.runnable(
										() -> {
											ConnectorNode connectorNode = getConnectorNode();
											if (null == connectorNode) {
												throw new NodeException("Node is stopped, need to exit write_record").context(getDataProcessorContext());
											}

											Consumer<WriteListResult<TapRecordEvent>> resultConsumer = (writeListResult) -> {
												if (obsLogger.isDebugEnabled()) {
													Map<TapRecordEvent, Throwable> errorMap = writeListResult.getErrorMap();
													if (MapUtils.isNotEmpty(errorMap)) {
														for (Map.Entry<TapRecordEvent, Throwable> tapRecordEventThrowableEntry : errorMap.entrySet()) {
															String errorRecordMsg = tapRecordEventThrowableEntry.getValue().getMessage() + "\n - Error record: " + tapRecordEventThrowableEntry.getKey()
																	+ "\n - Stack trace: " + Log4jUtil.getStackString(tapRecordEventThrowableEntry.getValue());
															obsLogger.debug(errorRecordMsg);
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
														writePolicyService.writeRecordWithPolicyControl(
																tapTable.getId(),
																subTapRecordEvents,
																writeRecords -> {
																	writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), writeRecords, tapTable, resultConsumer);
																	return null;
																}
														);
														return null;
													}));
											if (!pdkMethodInvoker.isEnableSkipErrorEvent()) {
												try {
													writePolicyService.writeRecordWithPolicyControl(
															tapTable.getId(),
															tapRecordEvents,
															writeRecords -> {
																writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), tapRecordEvents, tapTable, resultConsumer);
																return null;
															}
													);
												} catch (Exception e) {
													Throwable matched = CommonUtils.matchThrowable(e, TapCodeException.class);
													if (null != matched) {
														if (matched instanceof TapPdkBaseException) {
															((TapPdkBaseException) matched).setTableName(tapTable.getId());
														}else if(matched instanceof TapPdkRunnerUnknownException){
															((TapPdkRunnerUnknownException) matched).setTableName(tapTable.getId());
														}
														throw matched;
													}else {
														throw new TapCodeException(TaskTargetProcessorExCode_15.WRITE_RECORD_COMMON_FAILED, String.format("Execute PDK method: %s, tableName: %s", PDKMethod.TARGET_WRITE_RECORD, tapTable.getId()), e);
													}
												}
											}
										}
								)
						));
                syncMetricCollector.log(tapRecordEvents);
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
				pdkMethodInvoker.runnable(() -> {
							try {
								writeRecordFunction.writeRecord(
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
										});
							} catch (Exception e) {
								throwTapCodeException(e,new TapCodeException(TapExactlyOnceWriteExCode_22.WRITE_CACHE_FAILED));
							}
						}
				));
	}

	@Override
	protected boolean eventExactlyOnceWriteCheckExists(TapdataEvent tapdataEvent) {
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
		PDKMethodInvoker pdkMethodInvoker = exactlyOncePdkMethodInvokerMap.computeIfAbsent(Thread.currentThread().getName(), k -> createPdkMethodInvoker());
		AtomicBoolean result = new AtomicBoolean(false);
		PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_QUERY_BY_ADVANCE_FILTER,
				pdkMethodInvoker.runnable(
						() -> {
							try {
								queryByAdvanceFilterFunction.query(connectorNode.getConnectorContext(), tapAdvanceFilter, tapTable, rs -> {
									if (null != rs.getError()) {
										throw new TapCodeException(TapExactlyOnceWriteExCode_22.CHECK_CACHE_FAILED, "Check cache failed by filter: " + tapAdvanceFilter, rs.getError());
									}
									result.set(CollectionUtils.isNotEmpty(rs.getResults()));
								});
							} catch (Exception e) {
								throwTapCodeException(e, new TapCodeException(TapExactlyOnceWriteExCode_22.CHECK_CACHE_FAILED).dynamicDescriptionParameters(tapAdvanceFilter, ExactlyOnceUtil.EXACTLY_ONCE_CACHE_TABLE_NAME));
							}
						}
				));
		return result.get();
	}

	@Override
	public void doClose() throws TapCodeException {
		super.doClose();
		CommonUtils.ignoreAnyError(() -> {
			if (MapUtils.isNotEmpty(exactlyOncePdkMethodInvokerMap)) {
				for (PDKMethodInvoker pdkMethodInvoker : exactlyOncePdkMethodInvokerMap.values()) {
					if (null == pdkMethodInvoker) continue;
					pdkMethodInvoker.cancelRetry();
				}
				exactlyOncePdkMethodInvokerMap.clear();
				exactlyOncePdkMethodInvokerMap = null;
			}
		}, TAG);
	}

	@Override
	protected void updateDAG(TapdataEvent tapdataEvent) {
		super.updateDAG(tapdataEvent);
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

	@Override
	protected void processConnectorAfterSnapshot(TapTable tapTable) {
		if (null == tapTable) {
			return;
		}
		ConnectorNode connectorNode = this.getConnectorNode();
		if (null == connectorNode) {
			return;
		}
		ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
		if (null == connectorFunctions) {
			return;
		}
		AfterInitialSyncFunction afterInitialSyncFunction = connectorFunctions.getAfterInitialSyncFunction();
		if (null == afterInitialSyncFunction) {
			return;
		}
		try {
			afterInitialSyncFunction.afterInitialSync(connectorNode.getConnectorContext(), tapTable);
		} catch (Throwable e) {
			TapCodeException tapCodeException = new TapCodeException(TaskTargetProcessorExCode_15.PROCESS_CONNECTOR_AFTER_SNAPSHOT, e)
					.dynamicDescriptionParameters(tapTable.getId(), e.getMessage());
			errorHandle(tapCodeException);
		}
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.ExistsDataProcessEnum;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.common.task.SyncTypeEnum;
import io.tapdata.metrics.TaskSampleRetriever;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
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
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastTargetPdkDataNode extends HazelcastTargetPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkDataNode.class);

	private AtomicBoolean running;

	private ResetCounterSampler resetInputCounter;
	private CounterSampler inputCounter;
	private ResetCounterSampler resetInsertedCounter;
	private CounterSampler insertedCounter;
	private ResetCounterSampler resetUpdatedCounter;
	private CounterSampler updatedCounter;
	private ResetCounterSampler resetDeletedCounter;
	private CounterSampler deletedCounter;
	private SpeedSampler inputQPS;
	private AverageSampler timeCostAvg;

	public HazelcastTargetPdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void initSampleCollector() {
		super.initSampleCollector();

		// init statistic and sample related initialize
		// TODO: init outputCounter initial value
		Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList("inputTotal", "insertedTotal", "updatedTotal", "deletedTotal"));
		resetInputCounter = statisticCollector.getResetCounterSampler("inputTotal");
		inputCounter = sampleCollector.getCounterSampler("inputTotal");
		inputCounter.inc(values.getOrDefault("inputTotal", 0).longValue());
		resetInsertedCounter = statisticCollector.getResetCounterSampler("insertedTotal");
		insertedCounter = sampleCollector.getCounterSampler("insertedTotal");
		insertedCounter.inc(values.getOrDefault("insertedTotal", 0).longValue());
		resetUpdatedCounter = statisticCollector.getResetCounterSampler("updatedTotal");
		updatedCounter = sampleCollector.getCounterSampler("updatedTotal");
		updatedCounter.inc(values.getOrDefault("updatedTotal", 0).longValue());
		resetDeletedCounter = statisticCollector.getResetCounterSampler("deletedTotal");
		deletedCounter = sampleCollector.getCounterSampler("deletedTotal");
		deletedCounter.inc(values.getOrDefault("deletedTotal", 0).longValue());
		inputQPS = sampleCollector.getSpeedSampler("inputQPS");
		timeCostAvg = sampleCollector.getAverageSampler("timeCostAvg");

		statisticCollector.addSampler("replicateLag", () -> {
			Long ts = null;
			for (SyncProgress progress : syncProgressMap.values()) {
				if (null == progress.getSourceTime()) continue;
				if (ts == null || ts > progress.getSourceTime()) {
					ts = progress.getSourceTime();
				}
			}

			return ts == null ? 0 : System.currentTimeMillis() - ts;
		});
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		try {
			super.init(context);
			this.running = new AtomicBoolean(true);
			initTargetDB();
			// MILESTONE-INIT_TRANSFORMER-FINISH
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.FINISH);
		} catch (Exception e) {
			// MILESTONE-INIT_TRANSFORMER-ERROR
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw e;
		}
	}

	private void initTargetDB() {
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		Node<?> node = dataProcessorContext.getNode();
		ExistsDataProcessEnum existsDataProcessEnum = getExistsDataProcess(node);
		SyncProgress syncProgress = initSyncProgress(dataProcessorContext.getSubTaskDto().getAttrs());
		if (null != syncProgress) return;
		for (String tableId : tapTableMap.keySet()) {
			if (!this.running.get()) {
				break;
			}
			TapTable tapTable = tapTableMap.get(tableId);
			if (null == tapTable) {
				throw new RuntimeException("Init target node failed, table \"" + tableId + "\"'s schema is null");
			}
			dropTable(existsDataProcessEnum, tableId);
			createTable(tapTable);
			clearData(existsDataProcessEnum, tableId);
			createTargetIndex(node, tableId, tapTable);
		}
	}

	private void createTargetIndex(Node node, String tableId, TapTable tapTable) {
		CreateIndexFunction createIndexFunction = connectorNode.getConnectorFunctions().getCreateIndexFunction();
		if (null != createIndexFunction) {
			List<TapIndex> tapIndices = new ArrayList<>();
			TapIndex tapIndex = new TapIndex();
			List<TapIndexField> tapIndexFields = new ArrayList<>();
			List<String> updateConditionFields = null;
			if (node instanceof TableNode) {
				updateConditionFields = ((TableNode) node).getUpdateConditionFields();
			} else if (node instanceof DatabaseNode) {
				updateConditionFields = new ArrayList<>(tapTable.primaryKeys(true));
			}
			if (CollectionUtils.isNotEmpty(updateConditionFields)) {
				updateConditionFields.forEach(field -> {
					TapIndexField tapIndexField = new TapIndexField();
					tapIndexField.setName(field);
					tapIndexField.setFieldAsc(true);
					tapIndexFields.add(tapIndexField);
				});
				tapIndex.setIndexFields(tapIndexFields);
				tapIndices.add(tapIndex);
				TapCreateIndexEvent indexEvent = createIndexEvent(tableId, tapIndices);
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_CREATE_INDEX, () -> createIndexFunction.createIndex(connectorNode.getConnectorContext(), tapTable, indexEvent), TAG);
			}
		}
	}

	private void clearData(ExistsDataProcessEnum existsDataProcessEnum, String tableId) {
		if (SyncTypeEnum.CDC == syncType) return;
		if (existsDataProcessEnum == ExistsDataProcessEnum.REMOVE_DATE) {
			ClearTableFunction clearTableFunction = connectorNode.getConnectorFunctions().getClearTableFunction();
			Optional.ofNullable(clearTableFunction).ifPresent(func -> {
				TapClearTableEvent tapClearTableEvent = clearTableEvent(tableId);
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_CLEAR_TABLE, () -> func.clearTable(connectorNode.getConnectorContext(), tapClearTableEvent), TAG);
			});
		}
	}

	private void createTable(TapTable tapTable) {
		CreateTableFunction createTableFunction = connectorNode.getConnectorFunctions().getCreateTableFunction();
		Optional.ofNullable(createTableFunction).ifPresent(func -> {
			TapCreateTableEvent tapCreateTableEvent = createTableEvent(tapTable);
			final ClassLoader oldClassloader = Thread.currentThread().getContextClassLoader();
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_CREATE_TABLE, () -> {
				ClassLoader pdkClassloader = Thread.currentThread().getContextClassLoader();
				if (oldClassloader != null) {
					Thread.currentThread().setContextClassLoader(oldClassloader);
				}
				try {
					func.createTable(connectorNode.getConnectorContext(), tapCreateTableEvent);
				} finally {
					Thread.currentThread().setContextClassLoader(pdkClassloader);
				}
			}, TAG);
		});
	}

	private void dropTable(ExistsDataProcessEnum existsDataProcessEnum, String tableId) {
		if (SyncTypeEnum.CDC == syncType) return;
		if (existsDataProcessEnum == ExistsDataProcessEnum.DROP_TABLE) {
			DropTableFunction dropTableFunction = connectorNode.getConnectorFunctions().getDropTableFunction();
			TapDropTableEvent tapDropTableEvent = dropTableEvent(tableId);
			Optional.ofNullable(dropTableFunction).ifPresent(func -> PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_DROP_TABLE, () -> func.dropTable(connectorNode.getConnectorContext(), tapDropTableEvent), TAG));
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
	void processPdk(List<TapRecordEvent> tapEvents) {
		resetInputCounter.inc(tapEvents.size());
		inputCounter.inc(tapEvents.size());
		inputQPS.add(tapEvents.size());
		dispatchTapRecordEvents(tapEvents, dispatchEntity -> !dispatchEntity.getLastTapEvent().getTableId().equals(dispatchEntity.getCurrentTapEvent().getTableId()), events -> {
			TapRecordEvent firstEvent = events.get(0);
			String tableId = firstEvent.getTableId();
			TapTable tapTable;
			if (StringUtils.isNotBlank(tableName)) {
				tapTable = dataProcessorContext.getTapTableMap().get(tableName);
			} else {
				tapTable = dataProcessorContext.getTapTableMap().get(tableNameMap.get(tableId));
			}
			handleTapTablePrimaryKeys(tapTable);
			events.forEach(this::addPropertyForMergeEvent);
			WriteRecordFunction writeRecordFunction = connectorNode.getConnectorFunctions().getWriteRecordFunction();
			if (writeRecordFunction != null) {
				logger.debug("Write {} of record events, {}", tapEvents.size(), LoggerUtils.targetNodeMessage(connectorNode));
				long start = System.currentTimeMillis();
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TARGET_WRITE_RECORD,
						() -> writeRecordFunction.writeRecord(connectorNode.getConnectorContext(), events, tapTable, writeListResult -> {
							Map<TapRecordEvent, Throwable> errorMap = writeListResult.getErrorMap();
							if (MapUtils.isNotEmpty(errorMap)) {
								for (Map.Entry<TapRecordEvent, Throwable> tapRecordEventThrowableEntry : errorMap.entrySet()) {
									logger.error(tapRecordEventThrowableEntry.getValue().getMessage(), tapRecordEventThrowableEntry.getValue());
									logger.error("Error record: " + tapRecordEventThrowableEntry.getKey());
								}
								throw new RuntimeException("Write record failed, will stop task");
							}
							timeCostAvg.add(events.size(), System.currentTimeMillis() - start);
							resetInsertedCounter.inc(writeListResult.getInsertedCount());
							insertedCounter.inc(writeListResult.getInsertedCount());
							resetUpdatedCounter.inc(writeListResult.getModifiedCount());
							updatedCounter.inc(writeListResult.getModifiedCount());
							resetDeletedCounter.inc(writeListResult.getRemovedCount());
							deletedCounter.inc(writeListResult.getRemovedCount());
							logger.debug("Wrote {} of record events, {}", tapEvents.size(), LoggerUtils.targetNodeMessage(connectorNode));
						}), TAG);
			} else {
				throw new RuntimeException("Call write record, cause: writeRecord function in target node is null");
			}
		});
	}

	private void handleTapTablePrimaryKeys(TapTable tapTable) {
		if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.updateOrInsert.name()) && null != updateConditionFields) {
			// 设置逻辑主键
			tapTable.setLogicPrimaries(updateConditionFields);
		} else if (writeStrategy.equals(com.tapdata.tm.commons.task.dto.MergeTableProperties.MergeType.appendWrite.name())) {
			// 没有关联条件，清空主键信息
			tapTable.getNameFieldMap().values().forEach(v -> v.setPrimaryKeyPos(0));
			tapTable.setLogicPrimaries(null);
		}
	}

	private void dispatchTapRecordEvents(List<TapRecordEvent> tapEvents, Predicate<DispatchEntity> dispatchClause, Consumer<List<TapRecordEvent>> consumer) {
		DispatchEntity dispatchEntity = new DispatchEntity();
		List<TapRecordEvent> tempList = new ArrayList<>();
		for (TapRecordEvent tapEvent : tapEvents) {
			if (null == dispatchEntity.getLastTapEvent()) {
				dispatchEntity.setLastTapEvent(tapEvent);
			}
			dispatchEntity.setCurrentTapEvent(tapEvent);
			if (null != dispatchClause && dispatchClause.test(dispatchEntity)) {
				consumer.accept(tempList);
				tempList.clear();
				dispatchEntity.setLastTapEvent(tapEvent);
			}
			tempList.add(tapEvent);
		}
		if (CollectionUtils.isNotEmpty(tempList)) {
			consumer.accept(tempList);
		}
	}

	private static class DispatchEntity {
		private TapRecordEvent lastTapEvent;
		private TapRecordEvent currentTapEvent;

		public TapRecordEvent getLastTapEvent() {
			return lastTapEvent;
		}

		public void setLastTapEvent(TapRecordEvent lastTapEvent) {
			this.lastTapEvent = lastTapEvent;
		}

		public TapRecordEvent getCurrentTapEvent() {
			return currentTapEvent;
		}

		public void setCurrentTapEvent(TapRecordEvent currentTapEvent) {
			this.currentTapEvent = currentTapEvent;
		}
	}

	@Override
	public void close() throws Exception {
		try {
			if (null != connectorNode) {
				PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, () -> connectorNode.connectorStop(), TAG);
			}
		} finally {
			super.close();
		}
	}

	private void addPropertyForMergeEvent(TapEvent tapEvent) {
		if (null == tapEvent) return;
		Object info = tapEvent.getInfo(MergeInfo.EVENT_INFO_KEY);
		if (!(info instanceof MergeInfo)) return;
		MergeInfo mergeInfo = (MergeInfo) info;
		if (CollectionUtils.isEmpty(updateConditionFields)) return;
		MergeTableProperties currentProperty = mergeInfo.getCurrentProperty();
		if (null == currentProperty) return;
		if (null == currentProperty.getMergeType()) {
			currentProperty.setMergeType(MergeTableProperties.MergeType.valueOf(writeStrategy));
		}
		if (currentProperty.getMergeType() == MergeTableProperties.MergeType.appendWrite) return;
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
}

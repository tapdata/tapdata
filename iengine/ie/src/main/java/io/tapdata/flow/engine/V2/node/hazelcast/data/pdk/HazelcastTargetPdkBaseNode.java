package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.jet.core.Inbox;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.entity.*;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:58
 **/
public abstract class HazelcastTargetPdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastTargetPdkBaseNode.class);
	protected Map<String, SyncProgress> syncProgressMap = new ConcurrentHashMap<>();
	protected Map<String, String> tableNameMap;
	protected String tableName;
	private AtomicBoolean firstBatchEvent = new AtomicBoolean();
	private AtomicBoolean firstStreamEvent = new AtomicBoolean();
	protected List<String> updateConditionFields;
	protected String writeStrategy = "updateOrInsert";
	private AtomicBoolean flushOffset = new AtomicBoolean(false);

	public HazelcastTargetPdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		initMilestoneService(MilestoneContext.VertexType.DEST);
		// MILESTONE-INIT_TRANSFORMER-RUNNING
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.RUNNING);
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		super.init(context);
		try {
			createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
			connectorNodeInit(dataProcessorContext);
		} catch (Throwable e) {
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_TRANSFORMER, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw new RuntimeException(e);
		}
		Node<?> node = dataProcessorContext.getNode();
		if (node instanceof TableNode) {
			tableName = ((TableNode) node).getTableName();
			updateConditionFields = ((TableNode) node).getUpdateConditionFields();
			writeStrategy = ((TableNode) node).getWriteStrategy();
		} else if (node instanceof DatabaseNode) {
			tableNameMap = new HashMap<>();
			List<String> tableNames = null;
			List<SyncObjects> syncObjects = ((DatabaseNode) node).getSyncObjects();
			SyncObjects tableObjects = syncObjects.stream().filter(s -> s.getType().equals(com.tapdata.entity.dataflow.SyncObjects.TABLE_TYPE)).findFirst().orElse(null);
			if (null != tableObjects) {
				tableNames = new ArrayList<>(tableObjects.getObjectNames());
			}
			if (CollectionUtils.isEmpty(tableNames))
				throw new RuntimeException("Found database node's table list is empty, will stop task");
			String tableNameTransform = ((DatabaseNode) node).getTableNameTransform();
			String tablePrefix = ((DatabaseNode) node).getTablePrefix();
			String tableSuffix = ((DatabaseNode) node).getTableSuffix();
			tableNames.forEach(t -> {
				String targetTableName = t;
				if (StringUtils.isNotBlank(tablePrefix)) targetTableName = tablePrefix + targetTableName;
				if (StringUtils.isNotBlank(tableSuffix)) targetTableName = targetTableName + tableSuffix;
				targetTableName = Capitalized.convert(targetTableName, tableNameTransform);
				tableNameMap.put(t, targetTableName);
			});
		}
	}

	@Override
	final public void process(int ordinal, @NotNull Inbox inbox) {
		try {
			if (!inbox.isEmpty()) {
				while (isRunning()) {
					List<TapdataEvent> tapdataEvents = new ArrayList<>();
					final int count = inbox.drainTo(tapdataEvents, 1000);
					if (count > 0) {
						List<TapRecordEvent> tapEvents = new ArrayList<>();

						TapRecordEvent tapEvent = null;
						TapCodecsFilterManager codecsFilterManager = connectorNode.getCodecsFilterManager();
						TapdataEvent lastDmlTapdataEvent = null;
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							if (tapdataEvent instanceof TapdataHeartbeatEvent) {
								flushSyncProgressMap(tapdataEvent);
								saveToSnapshot();
							} else if (tapdataEvent instanceof TapdataCompleteSnapshotEvent) {
								// MILESTONE-WRITE_SNAPSHOT-FINISH
								MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.FINISH);
							} else if (tapdataEvent instanceof TapdataStartCdcEvent) {
								// MILESTONE-WRITE_CDC_EVENT-RUNNING
								MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.RUNNING);
								flushSyncProgressMap(tapdataEvent);
								saveToSnapshot();
							} else {
								SyncStage syncStage = tapdataEvent.getSyncStage();
								if (syncStage == SyncStage.INITIAL_SYNC && firstBatchEvent.compareAndSet(false, true)) {
									// MILESTONE-WRITE_SNAPSHOT-RUNNING
									MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_SNAPSHOT, MilestoneStatus.RUNNING);
								} else if (syncStage == SyncStage.CDC && firstStreamEvent.compareAndSet(false, true)) {
									// MILESTONE-WRITE_CDC_EVENT-FINISH
									MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.WRITE_CDC_EVENT, MilestoneStatus.FINISH);
								}
								if (tapdataEvent.getMessageEntity() != null) {
									tapEvent = message2TapEvent(tapdataEvent.getMessageEntity());
								} else if (null != tapdataEvent.getTapEvent()) {
									tapEvent = (TapRecordEvent) tapdataEvent.getTapEvent();
								}
								if (writeStrategy.equals(MergeTableProperties.MergeType.appendWrite.name())) {
									if (!(tapEvent instanceof TapInsertRecordEvent)) {
										continue;
									}
								}
								if (null == tapEvent) {
									logger.warn("Found unrecognized event, will skip it: " + tapdataEvent.getClass().getName() + ", " + tapdataEvent);
									continue;
								}
								fromTapValue(TapEventUtil.getBefore(tapEvent), codecsFilterManager);
								fromTapValue(TapEventUtil.getAfter(tapEvent), codecsFilterManager);
								tapEvents.add(tapEvent);
								if (null != tapdataEvent.getBatchOffset() || null != tapdataEvent.getStreamOffset()) {
									lastDmlTapdataEvent = tapdataEvent;
								}
							}
						}
						if (CollectionUtils.isNotEmpty(tapEvents)) {
							processPdk(tapEvents);
						}
						flushSyncProgressMap(lastDmlTapdataEvent);
					} else {
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error("Target process failed {}", e.getMessage(), e);
			throw sneakyThrow(e);
		}
	}

	private void flushSyncProgressMap(TapdataEvent tapdataEvent) {
		if (null == tapdataEvent) return;
		Node<?> node = processorBaseContext.getNode();
		if (CollectionUtils.isEmpty(tapdataEvent.getNodeIds())) return;
		String progressKey = tapdataEvent.getNodeIds().get(0) + "," + node.getId();
		SyncProgress syncProgress = this.syncProgressMap.get(progressKey);
		if (null == syncProgress) {
			syncProgress = new SyncProgress();
			this.syncProgressMap.put(progressKey, syncProgress);
		}
		if (tapdataEvent instanceof TapdataStartCdcEvent) {
			if (null == tapdataEvent.getSyncStage()) return;
			syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
		} else if (tapdataEvent instanceof TapdataHeartbeatEvent) {
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			flushOffset.set(true);
		} else {
			if (null == tapdataEvent.getSyncStage()) return;
			if (null == tapdataEvent.getBatchOffset() && null == tapdataEvent.getStreamOffset()) return;
			if (SyncStage.CDC == tapdataEvent.getSyncStage() && null == tapdataEvent.getSourceTime()) return;
			if (null != tapdataEvent.getBatchOffset()) {
				syncProgress.setBatchOffsetObj(tapdataEvent.getBatchOffset());
			}
			if (null != tapdataEvent.getStreamOffset()) {
				syncProgress.setStreamOffsetObj(tapdataEvent.getStreamOffset());
			}
			if (null != tapdataEvent.getSyncStage() && null != syncProgress.getSyncStage() && !syncProgress.getSyncStage().equals(SyncStage.CDC.name())) {
				syncProgress.setSyncStage(tapdataEvent.getSyncStage().name());
			}
			syncProgress.setSourceTime(tapdataEvent.getSourceTime());
			flushOffset.set(true);
		}
		syncProgress.setEventSerialNo(syncProgress.addAndGetSerialNo(1));
	}

	abstract void processPdk(List<TapRecordEvent> tapEvents);

	@Override
	public boolean saveToSnapshot() {
		if (!flushOffset.get()) return true;
		if (MapUtils.isEmpty(syncProgressMap)) return true;
		Map<String, String> syncProgressJsonMap = new HashMap<>(syncProgressMap.size());
		for (Map.Entry<String, SyncProgress> entry : syncProgressMap.entrySet()) {
			String key = entry.getKey();
			SyncProgress syncProgress = entry.getValue();
			List<String> list = Arrays.asList(key.split(","));
			if (null != syncProgress.getBatchOffsetObj()) {
				syncProgress.setBatchOffset(PdkUtil.encodeOffset(syncProgress.getBatchOffsetObj()));
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				syncProgress.setStreamOffset(PdkUtil.encodeOffset(syncProgress.getStreamOffsetObj()));
			}
			try {
				syncProgressJsonMap.put(JSONUtil.obj2Json(list), JSONUtil.obj2Json(syncProgress));
			} catch (JsonProcessingException e) {
				throw new RuntimeException("Convert offset to json failed, errors: " + e.getMessage(), e);
			}
		}
		SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
		String collection = ConnectorConstant.SUB_TASK_COLLECTION + "/syncProgress/" + subTaskDto.getId();
		try {
			clientMongoOperator.insertOne(syncProgressJsonMap, collection);
		} catch (Exception e) {
			throw new RuntimeException("Save to snapshot failed, collection: " + collection + ", object: " + this.syncProgressMap + "errors: " + e.getMessage(), e);
		}
		return true;
	}
}

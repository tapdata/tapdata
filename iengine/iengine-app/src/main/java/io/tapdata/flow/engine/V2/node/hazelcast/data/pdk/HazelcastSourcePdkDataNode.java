package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataStartCdcEvent;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.sharecdc.ReaderType;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcTaskPdkContext;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;
import io.tapdata.flow.engine.V2.sharecdc.impl.ShareCdcFactory;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.SchemaCacheUtil;
import io.tapdata.schema.TapTableMap;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastSourcePdkDataNode extends HazelcastSourcePdkBaseNode {
	private static final String TAG = HazelcastSourcePdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkDataNode.class);
	private ShareCdcReader shareCdcReader;

	public HazelcastSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		try {
			super.doInit(context);
			// MILESTONE-INIT_CONNECTOR-FINISH
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.FINISH);
		} catch (Throwable e) {
			// MILESTONE-INIT_CONNECTOR-ERROR
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw e;
		}
	}

	@Override
	public void startSourceRunner() {
		try {
			Node<?> node = dataProcessorContext.getNode();
			Thread.currentThread().setName("PDK-SOURCE-RUNNER-" + node.getName() + "(" + node.getId() + ")");
			Log4jUtil.setThreadContext(dataProcessorContext.getSubTaskDto());
			TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
			if (dataProcessorContext.getSubTaskDto().isTransformTask()) {
				// 测试任务
				long startTs = System.currentTimeMillis();
				for (String tableName : tapTableMap.keySet()) {
					if (!isRunning()) {
						break;
					}
					TapTable tapTable = tapTableMap.get(tableName);
					String sampleDataId = ((TableNode) node).getConnectionId() + "_" + tableName;
					SchemaCacheUtil.getSampleData(sampleDataId, getConnectorNode(), tapTable, TAG, tapEvents -> {
						List<TapdataEvent> tapdataEvents = wrapTapdataEvent(tapEvents);
						if (CollectionUtil.isNotEmpty(tapdataEvents)) {
							tapdataEvents.forEach(this::enqueue);
						}
					});
				}

				if (logger.isDebugEnabled()) {
					logger.info("query sample data complet, cost {}ms", (System.currentTimeMillis() - startTs));
				}
				return;
			}
			if (need2InitialSync(syncProgress)) {
				try {
					// MILESTONE-READ_SNAPSHOT-RUNNING
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.RUNNING);
					syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
					snapshotProgressManager = new SnapshotProgressManager(dataProcessorContext.getSubTaskDto(), clientMongoOperator,
							getConnectorNode(), dataProcessorContext.getTapTableMap());
					snapshotProgressManager.startStatsSnapshotEdgeProgress(dataProcessorContext.getNode());
					BatchReadFunction batchReadFunction = getConnectorNode().getConnectorFunctions().getBatchReadFunction();
					if (batchReadFunction != null) {
						for (String tableName : tapTableMap.keySet()) {
							if (!isRunning()) {
								break;
							}
							TapTable tapTable = tapTableMap.get(tableName);
							Object tableOffset = ((Map<String, Object>) syncProgress.getBatchOffsetObj()).get(tapTable.getId());
							logger.info("Starting batch read, table name: " + tapTable.getId() + ", offset: " + tableOffset);
							int eventBatchSize = 100;

							executeDataFuncAspect(BatchReadFuncAspect.class, () -> new BatchReadFuncAspect()
									.eventBatchSize(eventBatchSize)
									.connectorContext(getConnectorNode().getConnectorContext())
									.offsetState(tableOffset)
									.dataProcessorContext(this.getDataProcessorContext())
									.start()
									.table(tapTable), batchReadFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_BATCH_READ,
											() -> batchReadFunction.batchRead(getConnectorNode().getConnectorContext(), tapTable, tableOffset, eventBatchSize, (events, offsetObject) -> {
												if (events != null && !events.isEmpty()) {
													if (logger.isDebugEnabled()) {
														logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
													}
													((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(tapTable.getId(), offsetObject);
													List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events);

													if (CollectionUtil.isNotEmpty(tapdataEvents)) {
														tapdataEvents.forEach(this::enqueue);

														if(batchReadFuncAspect != null && batchReadFuncAspect.getConsumer() != null)
															batchReadFuncAspect.getConsumer().accept(tapdataEvents);
													}
												}
											}), TAG));
						}

						if (isRunning()) {
							// MILESTONE-READ_SNAPSHOT-FINISH
							MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.FINISH);
							enqueue(new TapdataCompleteSnapshotEvent());
						}
					} else {
						throw new RuntimeException("PDK node does not support batch read: " + dataProcessorContext.getDatabaseType());
					}
				} catch (Throwable e) {
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					throw e;
				} finally {
					snapshotProgressManager.close();
				}
			}

			if (need2CDC()) {
				try {
					doCdc();
				} catch (Throwable e) {
					// MILESTONE-READ_CDC_EVENT-FINISH
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					logger.error("Read CDC failed, error message: " + e.getMessage(), e);
					throw e;
				}
			}
		} catch (Throwable throwable) {
			error = throwable;
		} finally {
			this.running.set(false);
		}
	}

	@SneakyThrows
	private void doCdc() {
		if (null == syncProgress.getStreamOffsetObj()) {
			throw new RuntimeException("Starting stream read failed, errors: start point offset is null");
		} else {
			TapdataStartCdcEvent tapdataStartCdcEvent = new TapdataStartCdcEvent();
			tapdataStartCdcEvent.setSyncStage(SyncStage.CDC);
			enqueue(tapdataStartCdcEvent);
		}
		// MILESTONE-READ_CDC_EVENT-RUNNING
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
		syncProgress.setSyncStage(SyncStage.CDC.name());
		Node<?> node = dataProcessorContext.getNode();
		if (node.isLogCollectorNode()) {
			// Mining tasks force traditional increments
			doNormalCDC();
		} else {
			try {
				// Try to start with share cdc
				doShareCdc();
			} catch (ShareCdcUnsupportedException e) {
				if (e.isContinueWithNormalCdc()) {
					// If share cdc is unavailable, and continue with normal cdc is true
					logger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
					doNormalCDC();
				} else {
					throw new RuntimeException("Read share cdc log failed: " + e.getMessage(), e);
				}
			} catch (Exception e) {
				throw new RuntimeException("Read share cdc log failed: " + e.getMessage(), e);
			}
		}
	}

	private void doNormalCDC() {
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		StreamReadFunction streamReadFunction = getConnectorNode().getConnectorFunctions().getStreamReadFunction();
		if (streamReadFunction != null) {
			logger.info("Starting stream read, table list: " + tapTableMap.keySet() + ", offset: " + syncProgress.getOffsetObj());
			List<String> tables = new ArrayList<>(tapTableMap.keySet());
			int batchSize = 1;
			executeDataFuncAspect(StreamReadFuncAspect.class, () -> new StreamReadFuncAspect()
					.connectorContext(getConnectorNode().getConnectorContext())
					.dataProcessorContext(getDataProcessorContext())
					.tables(tables)
					.eventBatchSize(batchSize)
					.offsetState(syncProgress.getStreamOffsetObj())
					.start(), streamReadFuncAspect -> PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.SOURCE_STREAM_READ,
							() -> streamReadFunction.streamRead(getConnectorNode().getConnectorContext(), tables,
									syncProgress.getStreamOffsetObj(), batchSize, StreamReadConsumer.create((events, offsetObj) -> {
										if (events != null && !events.isEmpty()) {
											List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events, SyncStage.CDC, offsetObj);
											if (logger.isDebugEnabled()) {
												logger.debug("Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
											}
											if (CollectionUtils.isNotEmpty(tapdataEvents)) {
												tapdataEvents.forEach(this::enqueue);
												if(streamReadFuncAspect != null && streamReadFuncAspect.getConsumer() != null)
													streamReadFuncAspect.getConsumer().accept(tapdataEvents);
											}
										}
									}).stateListener((oldState, newState) -> {
										if (null != newState && StreamReadConsumer.STATE_STREAM_READ_STARTED == newState) {
											// MILESTONE-READ_CDC_EVENT-FINISH
											if(streamReadFuncAspect != null)
												executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAM_STARTED).streamStartedTime(System.currentTimeMillis()));
											MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
										}
									})), TAG));
		} else {
			throw new RuntimeException("PDK node does not support stream read: " + dataProcessorContext.getDatabaseType());
		}
	}

	private void doShareCdc() throws Exception {
		ShareCdcTaskContext shareCdcTaskContext = new ShareCdcTaskPdkContext(getCdcStartTs(), processorBaseContext.getConfigurationCenter(),
				dataProcessorContext.getSubTaskDto(), dataProcessorContext.getNode(), dataProcessorContext.getSourceConn(), getConnectorNode());
		logger.info("Starting incremental sync, read from share log storage...");
		// Init share cdc reader, if unavailable, will throw ShareCdcUnsupportedException
		this.shareCdcReader = ShareCdcFactory.shareCdcReader(ReaderType.PDK_TASK_HAZELCAST, shareCdcTaskContext);
		// Start listen message entity from share storage log
		this.shareCdcReader.listen((event, offsetObj) -> {
			TapdataEvent tapdataEvent = wrapTapdataEvent(event, SyncStage.CDC, offsetObj, true);
			if (null == tapdataEvent) {
				return;
			}
			tapdataEvent.setType(SyncProgress.Type.SHARE_CDC);
			enqueue(tapdataEvent);
		});
	}

	private Long getCdcStartTs() {
		Long cdcStartTs;
		try {
			if (null != this.syncProgress && null != this.syncProgress.getEventTime() && this.syncProgress.getEventTime().compareTo(0L) > 0) {
				cdcStartTs = this.syncProgress.getEventTime();
			} else {
				cdcStartTs = initialFirstStartTime;
			}
		} catch (Exception e) {
			throw new RuntimeException("Get cdc start ts failed; Error: " + e.getMessage(), e);
		}
		return cdcStartTs;
	}

	@Override
	public void doClose() throws Exception {
		try {
			if (null != getConnectorNode()) {
				PDKInvocationMonitor.invoke(getConnectorNode(), PDKMethod.STOP, () -> getConnectorNode().connectorStop(), TAG);
			}
		} finally {
			super.doClose();
		}
	}
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.CollectionUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.entity.SyncStage;
import io.tapdata.flow.engine.V2.entity.TapdataCompleteSnapshotEvent;
import io.tapdata.flow.engine.V2.entity.TapdataEvent;
import io.tapdata.flow.engine.V2.entity.TapdataStartCdcEvent;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.metrics.TaskSampleRetriever;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 * @date 2022/2/22 2:33 PM
 **/
public class HazelcastSourcePdkDataNode extends HazelcastSourcePdkBaseNode {
	private static final String TAG = HazelcastSourcePdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkDataNode.class);
	private ResetCounterSampler resetOutputCounter;
	private CounterSampler outputCounter;
	private SpeedSampler outputQPS;
	private ResetCounterSampler resetInitialWriteCounter;
	private CounterSampler initialWriteCounter;
	private Long initialTime;

	public HazelcastSourcePdkDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		try {
			super.init(context);
			Node<?> node = dataProcessorContext.getNode();
			if (node instanceof TableNode) {
				// do nothing
			} else if (node instanceof DatabaseNode) {
				// do nothing
			} else {
				throw new IllegalArgumentException("Expect node type: TableNode, actual: " + node.getClass().getName());
			} // MILESTONE-INIT_CONNECTOR-FINISH
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
			if (need2InitialSync(syncProgress)) {
				try {
					// MILESTONE-READ_SNAPSHOT-RUNNING
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_SNAPSHOT, MilestoneStatus.RUNNING);
					syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
					snapshotProgressManager = new SnapshotProgressManager(dataProcessorContext.getSubTaskDto(), clientMongoOperator,
							connectorNode, dataProcessorContext.getTapTableMap());
					snapshotProgressManager.startStatsSnapshotEdgeProgress(dataProcessorContext.getNode());
					BatchReadFunction batchReadFunction = connectorNode.getConnectorFunctions().getBatchReadFunction();
					if (batchReadFunction != null) {
						for (String tableName : tapTableMap.keySet()) {
							if (!isRunning()) {
								break;
							}
							TapTable tapTable = tapTableMap.get(tableName);
							Object tableOffset = ((Map<String, Object>) syncProgress.getBatchOffsetObj()).get(tapTable.getId());
							logger.info("Starting batch read, table name: " + tapTable.getId() + ", offset: " + tableOffset);
							PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_BATCH_READ,
									() -> batchReadFunction.batchRead(connectorNode.getConnectorContext(), tapTable, tableOffset, 100, (events, offsetObject) -> {
										if (events != null && !events.isEmpty()) {
											if (logger.isDebugEnabled()) {
												logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(connectorNode));
											}
											((Map<String, Object>) syncProgress.getBatchOffsetObj()).put(tapTable.getId(), offsetObject);
											List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events, SyncStage.INITIAL_SYNC);
											if (CollectionUtil.isNotEmpty(tapdataEvents)) {
												tapdataEvents.forEach(this::enqueue);
												resetOutputCounter.inc(tapdataEvents.size());
												outputCounter.inc(tapdataEvents.size());
												outputQPS.add(tapdataEvents.size());
												resetInitialWriteCounter.inc(tapdataEvents.size());
												initialWriteCounter.inc(tapdataEvents.size());
											}
										}
									}), TAG);
						}

						if (isRunning()) {
							initialTime = System.currentTimeMillis();
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
				if (null == syncProgress.getStreamOffsetObj()) {
					throw new RuntimeException("Starting stream read failed, errors: start point offset is null");
				} else {
					TapdataStartCdcEvent tapdataStartCdcEvent = new TapdataStartCdcEvent();
					tapdataStartCdcEvent.setSyncStage(SyncStage.CDC);
					enqueue(tapdataStartCdcEvent);
				}
				try {
					// MILESTONE-READ_CDC_EVENT-RUNNING
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
					syncProgress.setSyncStage(SyncStage.CDC.name());
					StreamReadFunction streamReadFunction = connectorNode.getConnectorFunctions().getStreamReadFunction();
					if (streamReadFunction != null) {
						logger.info("Starting stream read, table list: " + tapTableMap.keySet() + ", offset: " + syncProgress.getOffsetObj());
						PDKInvocationMonitor.invoke(connectorNode, PDKMethod.SOURCE_STREAM_READ,
								() -> streamReadFunction.streamRead(connectorNode.getConnectorContext(), new ArrayList<>(tapTableMap.keySet()),
										syncProgress.getStreamOffsetObj(), 1, StreamReadConsumer.create((events, offsetObj) -> {
											if (events != null && !events.isEmpty()) {
												List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events, SyncStage.CDC, offsetObj);
												if (logger.isDebugEnabled()) {
													logger.debug("Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(connectorNode));
												}
												if (CollectionUtils.isNotEmpty(tapdataEvents)) {
													tapdataEvents.forEach(this::enqueue);
													resetOutputCounter.inc(tapdataEvents.size());
													outputCounter.inc(tapdataEvents.size());
													outputQPS.add(tapdataEvents.size());
												}
											}
										}).stateListener((oldState, newState) -> {
											if (null != newState && StreamReadConsumer.STATE_STREAM_READ_STARTED == newState) {
												// MILESTONE-READ_CDC_EVENT-FINISH
												MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
											}
										})), TAG);
					} else {
						throw new RuntimeException("PDK node does not support stream read: " + dataProcessorContext.getDatabaseType());
					}
				} catch (Throwable e) {
					// MILESTONE-READ_CDC_EVENT-FINISH
					MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
					throw e;
				}
			}
		} catch (Throwable throwable) {
			error = throwable;
		} finally {
			this.running.set(false);
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

	/**
	 * TODO(dexter): restore from the db;
	 */
	@Override
	protected void initSampleCollector() {
		super.initSampleCollector();

		// TODO: init outputCounter initial value
		Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
				"outputTotal", "initialWrite"
		));
		// init statistic and sample related initialize
		resetOutputCounter = statisticCollector.getResetCounterSampler("outputTotal");
		outputCounter = sampleCollector.getCounterSampler("outputTotal");
		outputCounter.inc(values.getOrDefault("outputTotal", 0).longValue());
		outputQPS = sampleCollector.getSpeedSampler("outputQPS");
		resetInitialWriteCounter = statisticCollector.getResetCounterSampler("initialWrite");
		initialWriteCounter = sampleCollector.getCounterSampler("initialWrite");
		initialWriteCounter.inc(values.getOrDefault("initialWrite", 0).longValue());

		statisticCollector.addSampler("initialTime", () -> {
			if (initialTime != null) {
				return initialTime;
			}
			return 0;
		});
		if (syncProgress != null) {
			statisticCollector.addSampler("cdcTime", () -> syncProgress.getEventTime());
		}
	}
}

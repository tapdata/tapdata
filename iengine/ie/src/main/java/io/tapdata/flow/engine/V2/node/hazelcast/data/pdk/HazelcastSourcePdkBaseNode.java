package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.entity.dataflow.SyncProgress;
import com.tapdata.entity.task.NodeUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.SourceException;
import io.tapdata.flow.engine.V2.entity.SyncStage;
import io.tapdata.flow.engine.V2.entity.TapdataEvent;
import io.tapdata.flow.engine.V2.entity.TapdataHeartbeatEvent;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.milestone.MilestoneContext;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-05-11 14:59
 **/
public abstract class HazelcastSourcePdkBaseNode extends HazelcastPdkBaseNode {
	private static final String TAG = HazelcastTargetPdkDataNode.class.getSimpleName();
	private final Logger logger = LogManager.getLogger(HazelcastSourcePdkBaseNode.class);
	protected SyncProgress syncProgress;
	protected ExecutorService sourceRunner = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>());
	protected SnapshotProgressManager snapshotProgressManager;
	protected LinkedBlockingQueue<TapdataEvent> eventQueue = new LinkedBlockingQueue<>(10);
	private TapdataEvent pendingEvent;

	public HazelcastSourcePdkBaseNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		initMilestoneService(MilestoneContext.VertexType.SOURCE);
		// MILESTONE-INIT_CONNECTOR-RUNNING
		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.RUNNING);
	}

	@Override
	protected void init(@NotNull Context context) throws Exception {
		super.init(context);
		try {
			createPdkConnectorNode(dataProcessorContext, context.hazelcastInstance());
			connectorNodeInit(dataProcessorContext);
		} catch (Throwable e) {
			MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.INIT_CONNECTOR, MilestoneStatus.ERROR, e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			throw new RuntimeException(e);
		}
		SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
		syncProgress = initSyncProgress(subTaskDto.getAttrs());
		if (syncProgress == null) {
			syncProgress = new SyncProgress();
			syncProgress.setBatchOffsetObj(new HashMap<>());
			// null present current
			Long offsetStartTimeMs = null;
			switch (syncType) {
				case INITIAL_SYNC_CDC:
					initStreamOffsetFromTime(offsetStartTimeMs);
					break;
				case INITIAL_SYNC:
					syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
					break;
				case CDC:
					List<TaskDto.SyncPoint> syncPoints = subTaskDto.getParentTask().getSyncPoints();
					String connectionId = NodeUtil.getConnectionId(dataProcessorContext.getNode());
					TaskDto.SyncPoint syncPoint = null;
					if (null != syncPoints) {
						syncPoint = syncPoints.stream().filter(sp -> sp.getConnectionId().equals(connectionId)).findFirst().orElse(null);
					}
					if (null == syncPoint) {
						throw new RuntimeException("Cannot found sync point setting in task, connection id: " + connectionId);
					}
					String pointType = syncPoint.getPointType();
					switch (pointType) {
						case "localTZ":
						case "connTZ":
							// todo missing db timezone
							offsetStartTimeMs = syncPoint.getDateTime();
							break;
						case "current":
							break;
					}
					initStreamOffsetFromTime(offsetStartTimeMs);
					break;
			}
			if (null != syncProgress.getStreamOffsetObj()) {
				TapdataEvent tapdataEvent = new TapdataHeartbeatEvent(offsetStartTimeMs, syncProgress.getStreamOffsetObj());
				enqueue(tapdataEvent);
			}
		} else {
			String batchOffset = syncProgress.getBatchOffset();
			if (StringUtils.isNotBlank(batchOffset)) {
				syncProgress.setBatchOffsetObj(PdkUtil.decodeOffset(batchOffset, connectorNode));
			} else {
				syncProgress.setBatchOffsetObj(new HashMap<>());
			}
			String streamOffset = syncProgress.getStreamOffset();
			if (StringUtils.isNotBlank(streamOffset)) {
				syncProgress.setStreamOffsetObj(PdkUtil.decodeOffset(streamOffset, connectorNode));
			} else {
				initStreamOffsetFromTime(null);
			}
		}
		this.sourceRunner.submit(this::startSourceRunner);
	}

	private void initStreamOffsetFromTime(Long offsetStartTimeMs) {
		AtomicReference<Object> timeToStreamOffsetResult = new AtomicReference<>();
		TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = connectorNode.getConnectorFunctions().getTimestampToStreamOffsetFunction();
		if (null != timestampToStreamOffsetFunction) {
			PDKInvocationMonitor.invoke(connectorNode, PDKMethod.TIMESTAMP_TO_STREAM_OFFSET, () -> {
				try {
					timeToStreamOffsetResult.set(timestampToStreamOffsetFunction.timestampToStreamOffset(connectorNode.getConnectorContext(), offsetStartTimeMs));
				} catch (Throwable e) {
					logger.warn("Call timestamp to stream offset function failed, will stop task after snapshot, type: " + dataProcessorContext.getDatabaseType()
							+ ", errors: " + e.getClass().getSimpleName() + "  " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
				}
				syncProgress.setStreamOffsetObj(timeToStreamOffsetResult.get());
			}, TAG);
		} else {
			logger.warn("Pdk connector does not support timestamp to stream offset function, will stop task after snapshot: " + dataProcessorContext.getDatabaseType());
		}
	}

	@Override
	final public boolean complete() {
		try {
			SubTaskDto subTaskDto = dataProcessorContext.getSubTaskDto();
			Log4jUtil.setThreadContext(subTaskDto);
			TapdataEvent dataEvent;
			AtomicBoolean isPending = new AtomicBoolean();
			if (!isRunning()) {
				return true;
			}
			if (pendingEvent != null) {
				dataEvent = pendingEvent;
				pendingEvent = null;
				isPending.compareAndSet(false, true);
			} else {
				dataEvent = eventQueue.poll(5, TimeUnit.SECONDS);
				isPending.compareAndSet(true, false);
			}

			if (dataEvent != null) {
				TapEvent tapEvent;
				if (!isPending.get()) {
					TapCodecsFilterManager codecsFilterManager = this.connectorNode.getCodecsFilterManager();
					tapEvent = dataEvent.getTapEvent();
					tapRecordToTapValue(tapEvent, codecsFilterManager);
				}
				if (!offer(dataEvent)) {
					pendingEvent = dataEvent;
					return false;
				}
				Optional.ofNullable(snapshotProgressManager)
						.ifPresent(s -> s.incrementEdgeFinishNumber(TapEventUtil.getTableId(dataEvent.getTapEvent())));
			}

			if (error != null) {
				throw new RuntimeException(error);
			}
		} catch (Exception e) {
			logger.error("Source sync failed {}.", e.getMessage(), e);
			throw new SourceException(e, true);
		}

		return false;
	}

	abstract void startSourceRunner();

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events, SyncStage syncStage) {
		return wrapTapdataEvent(events, syncStage, null);
	}

	@NotNull
	protected List<TapdataEvent> wrapTapdataEvent(List<TapEvent> events, SyncStage syncStage, Object offsetObj) {
		List<TapdataEvent> tapdataEvents = new ArrayList<>(events.size() + 1);
		for (int i = 0; i < events.size(); i++) {
			TapEvent tapEvent = events.get(i);
			TapdataEvent tapdataEvent = null;
			if (tapEvent instanceof TapRecordEvent) {
				TapRecordEvent tapRecordEvent = (TapRecordEvent) tapEvent;
				tapdataEvent = new TapdataEvent();
				tapdataEvent.setTapEvent(tapRecordEvent);
				tapdataEvent.setSyncStage(syncStage);
				if (SyncStage.INITIAL_SYNC == syncStage) {
					if (i == events.size() - 1) {
						Map<String, Object> batchOffsetObj = (Map<String, Object>) syncProgress.getBatchOffsetObj();
						Map<String, Object> newMap = new HashMap<>();
						try {
							MapUtil.deepCloneMap(batchOffsetObj, newMap);
						} catch (IllegalAccessException | InstantiationException e) {
							throw new RuntimeException("Deep clone batch offset map failed: " + e.getMessage(), e);
						}
						tapdataEvent.setBatchOffset(newMap);
					}
				} else if (SyncStage.CDC == syncStage) {
					tapdataEvent.setStreamOffset(offsetObj);
					if (null == ((TapRecordEvent) tapEvent).getReferenceTime())
						throw new RuntimeException("Tap CDC event's reference time is null");
					tapdataEvent.setSourceTime(((TapRecordEvent) tapEvent).getReferenceTime());
				}
			} else if (tapEvent instanceof HeartbeatEvent) {
				tapdataEvent = new TapdataHeartbeatEvent(((HeartbeatEvent) tapEvent).getReferenceTime(), offsetObj);
			}
			if (null == tapdataEvent) {
				throw new RuntimeException("Found event type does not support: " + tapEvent.getClass().getSimpleName());
			}
			tapdataEvent.addNodeId(dataProcessorContext.getNode().getId());
			tapdataEvents.add(tapdataEvent);
		}
		return tapdataEvents;
	}

	protected void enqueue(TapdataEvent tapdataEvent) {
		while (isRunning()) {
			try {
				if (eventQueue.offer(tapdataEvent, 3, TimeUnit.SECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	@Override
	public void close() throws Exception {
		Optional.ofNullable(sourceRunner).ifPresent(ExecutorService::shutdownNow);
		super.close();
	}

	public LinkedBlockingQueue<TapdataEvent> getEventQueue() {
		return eventQueue;
	}

	public SnapshotProgressManager getSnapshotProgressManager() {
		return snapshotProgressManager;
	}
}

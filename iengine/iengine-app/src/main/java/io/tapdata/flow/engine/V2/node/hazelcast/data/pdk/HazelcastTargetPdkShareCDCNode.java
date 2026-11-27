package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.TapdataShareLogEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import io.tapdata.entity.TapProcessorNodeContext;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.throttle.TaskThrottleRuntime;
import io.tapdata.flow.engine.V2.throttle.TaskThrottleRuntimeRegistry;
import io.tapdata.node.pdk.processor.TapProcessorNode;
import io.tapdata.node.pdk.processor.TapTargetShareCDCNode;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.jetbrains.annotations.NotNull;
import java.util.*;
import java.util.function.Function;

/**
 * @author samuel
 * @Description
 * @create 2022-06-14 17:23
 **/
public class HazelcastTargetPdkShareCDCNode extends HazelcastTargetPdkBaseNode {

	public static final String TAG = HazelcastTargetPdkShareCDCNode.class.getSimpleName();
	private TapProcessorNode processorNode;
	public HazelcastTargetPdkShareCDCNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		this.processorNode=new TapTargetShareCDCNode(dataProcessorContext);
	}

	/**
	 * Allows subclasses and unit tests on OSS classpath to supply a delegate {@link TapProcessorNode}
	 * without triggering the OSS stub constructor.
	 */
	protected HazelcastTargetPdkShareCDCNode(DataProcessorContext dataProcessorContext, TapProcessorNode processorNode) {
		super(dataProcessorContext);
		this.processorNode = processorNode;
	}

	@Override
	protected void doInit(@NotNull Context context) throws TapCodeException {
		super.doInit(context);
		TapProcessorNodeContext tapProcessorNodeContext = initTapProcessorNodeContext();
		this.targetBatch = 10000;
		this.targetBatchIntervalMs = 1000;
		this.processorNode.doInit(context,tapProcessorNodeContext);
		this.illegalDateAcceptable = true;
	}

	protected TapProcessorNodeContext initTapProcessorNodeContext() {
		return TapProcessorNodeContext.builder()
				.codecsFilterManager(codecsFilterManager)
				.running(running)
				.externalStorageDto(externalStorageDto)
				.obsLogger(obsLogger)
				.jetContext(jetContext)
				.build();
	}

	@Override
	void processEvents(List<TapEvent> tapEvents) {
		throw new UnsupportedOperationException();
	}

	@Override
	void processShareLog(List<TapdataShareLogEvent> tapdataShareLogEvents) {
		if (tapdataShareLogEvents == null || tapdataShareLogEvents.isEmpty()) {
			return;
		}
		TaskThrottleRuntime throttleRuntime = resolveThrottleRuntime();
		if (throttleRuntime == null) {
			this.processorNode.processShareLog(tapdataShareLogEvents);
			return;
		}
		IdentityHashMap<TapEvent, TapdataShareLogEvent> wrapperByEvent = new IdentityHashMap<>();
		List<TapEvent> tapEvents = new ArrayList<>(tapdataShareLogEvents.size());
		List<TapdataShareLogEvent> passthrough = new ArrayList<>();
		for (TapdataShareLogEvent ev : tapdataShareLogEvents) {
			if (ev == null) {
				continue;
			}
			if (ev.getTapEvent() != null) {
				TapEvent tapEvent = ev.getTapEvent();
				wrapperByEvent.put(tapEvent, ev);
				tapEvents.add(tapEvent);
			} else {
				passthrough.add(ev);
			}
		}
		if (!tapEvents.isEmpty()) {
			String targetNodeId = getNode() != null ? getNode().getId() : "";
			String targetEndpointId = (externalStorageDto != null && externalStorageDto.getId() != null)
					? externalStorageDto.getId().toHexString()
					: targetNodeId;
			Function<TapEvent, String> tableNormalizer = event -> {
				TapdataShareLogEvent orig = wrapperByEvent.get(event);
				String tableId = (event instanceof TapRecordEvent rec && rec.getTableId() != null)
						? rec.getTableId()
						: (event instanceof TapBaseEvent base && base.getTableId() != null ? base.getTableId() : "");
				if (tableId.isEmpty()) {
					return "";
				}
				Object connObj = orig != null ? orig.getInfo(TapdataEvent.CONNECTION_ID_INFO_KEY) : null;
				if (connObj == null && event != null && event.getInfo() != null) {
					connObj = event.getInfo().get(TapdataEvent.CONNECTION_ID_INFO_KEY);
				}
				if (connObj instanceof String connId && !connId.isBlank()) {
					return connId + ":" + tableId;
				}
				return tableId;
			};
			try {
				throttleRuntime.executeTargetWriteChunked(
						targetNodeId,
						targetEndpointId,
						ThrottlePhase.CDC,
						tapEvents,
						tableNormalizer,
						true,
						() -> !isRunning(),
						chunk -> {
							List<TapdataShareLogEvent> chunkShareEvents = new ArrayList<>(chunk.size());
							for (TapEvent chunkEvent : chunk) {
								TapdataShareLogEvent orig = wrapperByEvent.get(chunkEvent);
								if (orig != null) {
									chunkShareEvents.add(orig);
								}
							}
							if (!chunkShareEvents.isEmpty()) {
								this.processorNode.processShareLog(chunkShareEvents);
							}
						}
				);
			} catch (RuntimeException re) {
				throw re;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (!passthrough.isEmpty()) {
			this.processorNode.processShareLog(passthrough);
		}
	}

	protected TaskThrottleRuntime resolveThrottleRuntime() {
		TaskDto taskDto = dataProcessorContext != null ? dataProcessorContext.getTaskDto() : null;
		String taskId = (taskDto != null && taskDto.getId() != null) ? taskDto.getId().toHexString() : null;
		return taskId == null ? null : TaskThrottleRuntimeRegistry.getInstance().getActive(taskId);
	}


	@Override
	public void doClose() throws TapCodeException {
		CommonUtils.ignoreAnyError(()-> this.processorNode.doClose(), TAG);
		super.doClose();
	}
}

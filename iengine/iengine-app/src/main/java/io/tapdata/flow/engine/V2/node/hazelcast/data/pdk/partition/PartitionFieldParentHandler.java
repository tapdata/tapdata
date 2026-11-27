package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import cn.hutool.crypto.digest.MD5;
import com.tapdata.constant.CollectionUtil;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePdkDataNode;
import io.tapdata.flow.engine.V2.throttle.TaskThrottleRuntime;
import io.tapdata.flow.engine.V2.throttle.TaskThrottleRuntimeRegistry;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.simplify.pretty.TypeHandlers;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.HazelcastSourcePartitionReadDataNode;
import io.tapdata.pdk.core.utils.LoggerUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.map;

/**
 * @author aplomb
 */
public class PartitionFieldParentHandler {
	public static final String TAG = PartitionFieldParentHandler.class.getName();
	protected String table;
	protected List<String> partitionFields;
	protected Map<String, Integer> dateFieldFactionMap;
	protected TypeHandlers<TapEvent, Void> typeHandlers = new TypeHandlers<>();


	public PartitionFieldParentHandler(TapTable tapTable) {
		table = tapTable.getId();
		TapIndexEx partitionIndex = tapTable.partitionIndex();
//		if(partitionIndex == null || partitionIndex.getIndexFields() == null || partitionIndex.getIndexFields().isEmpty()) {
//			//TapLogger.warn(TAG, "Not find any primary keys, partition index is illegal for table {}.", table);
//			throw new CoreException(PartitionErrorCodes.PARTITION_INDEX_NULL, "Not find any primary keys, partition index is illegal for table {}, cancel full breakpoint resume.", table);
//			//partitionFields.addAll(nameFieldMap.keySet());
//			//if (partitionFields.isEmpty()){
//			//	throw new CoreException(PartitionErrorCodes.PARTITION_INDEX_NULL,"Not find any fields in source table {}.", table);
//			//}
//			//return;
//		}
		partitionFields = new ArrayList<>();
		dateFieldFactionMap = new HashMap<>();
		if (partitionIndex != null) {
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			for (TapIndexField field : partitionIndex.getIndexFields()) {
				partitionFields.add(field.getName());
				if (nameFieldMap != null) {
					TapField tapField = nameFieldMap.get(field.getName());
					if (tapField != null && tapField.getTapType() != null) {
						if (tapField.getTapType() instanceof TapDateTime) {
							Integer fraction = ((TapDateTime) tapField.getTapType()).getFraction();
							if (fraction == null)
								fraction = 3;
							dateFieldFactionMap.put(field.getName(), fraction);
						} else if (tapField.getTapType() instanceof TapDate || tapField.getTapType() instanceof TapTime || tapField.getTapType() instanceof TapYear) {
							dateFieldFactionMap.put(field.getName(), 3);
						}
					}
				}
			}
		}
	}

	protected Map<String, Object> getKeyFromData(Map<String, Object> before) {
		return getKeyFromData(before, null);
	}

	protected Map<String, Object> getKeyFromData(Map<String, Object> before, Map<String, Object> after) {
		Map<String, Object> map = map();
		if (partitionFields == null || partitionFields.isEmpty()) {
			Map<String, Object> data = null;
			if (before != null) {
				data = before;
			} else if (after != null) {
				data = after;
			}
			if (data != null) {
				TreeMap<String, Object> sortMap = new TreeMap<>(String::compareTo);
				sortMap.putAll(data);
				String md5 = MD5.create().digestHex(InstanceFactory.instance(ObjectSerializable.class).fromObject(sortMap));
				map.put("md5", md5);
			}
		} else {
			if (before != null) {
				for (String field : partitionFields) {
					map.put(field, before.get(field));
				}
			} else if (after != null) {
				for (String field : partitionFields) {
					map.put(field, after.get(field));
				}
			}
		}
		return map;
	}

	protected boolean checkKeyChanged(Map<String, Object> before, Map<String, Object> after) {
		if (before == null || after == null)
			return true;

		for (String field : partitionFields) {
			Object afterF = after.get(field);
			Object beforeF = before.get(field);
			if (afterF != null && beforeF != null) {
				boolean bool = afterF.equals(beforeF);
				if (!bool)
					return false;
			} else {
				return true;
			}
		}
		return true;
	}

	/**
	 * As PDK connector may give different types for date field on batch/stream stages.
	 * <p>
	 * Make it the same to DateTime.
	 *
	 * @param data
	 * @return
	 */
	protected Map<String, Object> reviseData(Map<String, Object> data) {
		if (data != null) {
			if (dateFieldFactionMap != null && !dateFieldFactionMap.isEmpty()) {
				for (Map.Entry<String, Integer> entry : dateFieldFactionMap.entrySet()) {
					data.put(entry.getKey(), AnyTimeToDateTime.toDateTime(data.get(entry.getKey()), entry.getValue()));
				}
			}
		}
		return data;
	}

	protected void enqueueTapEvents(BatchReadFuncAspect batchReadFuncAspect, List<TapEvent> events, HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
		if (events == null || events.isEmpty())
			return;
//		long time = System.currentTimeMillis();
		if (batchReadFuncAspect != null)
			AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_READ_COMPLETE).getReadCompleteConsumers(), events);
//		sourcePdkDataNode.getObsLogger().trace("STATE_READ_COMPLETE events {} takes {}", events.size(), (System.currentTimeMillis() - time));

		if (sourcePdkDataNode.logger.isDebugEnabled()) {
			sourcePdkDataNode.logger.debug("Batch read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(sourcePdkDataNode.getConnectorNode()));
		}

		DataProcessorContext ctx = sourcePdkDataNode.getDataProcessorContext();
		TaskDto taskDto = ctx != null ? ctx.getTaskDto() : null;
		String taskId = (taskDto != null && taskDto.getId() != null) ? taskDto.getId().toHexString() : null;
		TaskThrottleRuntime throttleRuntime = taskId == null ? null : TaskThrottleRuntimeRegistry.getInstance().getActive(taskId);
		if (throttleRuntime == null) {
			List<TapdataEvent> tapdataEvents = sourcePdkDataNode.wrapTapdataEvent(events);

			if (batchReadFuncAspect != null)
				AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);

			if (CollectionUtils.isNotEmpty(tapdataEvents)) {
				tapdataEvents.forEach(tapdataEvent -> {
					if (tapdataEvent.getTapEvent() instanceof TapRecordEvent) {
						String tableId = ((TapRecordEvent) tapdataEvent.getTapEvent()).getTableId();
						if (sourcePdkDataNode.removeTables() != null && sourcePdkDataNode.removeTables().contains(tableId)) {
							return;
						}
					}
					sourcePdkDataNode.enqueue(tapdataEvent);
				});
				if (batchReadFuncAspect != null)
					AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
			}
			return;
		}

		List<String> removed = sourcePdkDataNode.removeTables();
		List<TapEvent> filtered = new ArrayList<>(events.size());
		for (TapEvent ev : events) {
			if (ev instanceof TapRecordEvent rec && removed != null && removed.contains(rec.getTableId())) {
				continue;
			}
			filtered.add(ev);
		}
		if (filtered.isEmpty()) {
			return;
		}
		String sourceNodeId = sourcePdkDataNode.getNode() != null ? sourcePdkDataNode.getNode().getId() : "";
		String endpointId = (ctx != null && ctx.getConnections() != null && ctx.getConnections().getId() != null)
				? String.valueOf(ctx.getConnections().getId()) : sourceNodeId;
		TaskThrottleRuntime.AdmitOutcome admitOutcome = throttleRuntime.admitSourceBatch(
				sourceNodeId,
				endpointId,
				ThrottlePhase.FULL,
				filtered,
				HazelcastSourcePdkDataNode::throttleTableNormalizer,
				() -> !sourcePdkDataNode.isNodeRunning(),
				slice -> {
					List<TapdataEvent> tapdataEvents = sourcePdkDataNode.wrapTapdataEvent(slice.events());
					if (batchReadFuncAspect != null)
						AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_PROCESS_COMPLETE).getProcessCompleteConsumers(), tapdataEvents);
					if (CollectionUtils.isNotEmpty(tapdataEvents)) {
						String leaseId = slice.lease() != null ? slice.lease().getLeaseId() : null;
						for (TapdataEvent tapdataEvent : tapdataEvents) {
							TaskThrottleRuntime.attachLeaseInfo(tapdataEvent, leaseId, slice.sourceNodeId(), slice.batchSeq());
							sourcePdkDataNode.enqueue(tapdataEvent);
						}
						if (batchReadFuncAspect != null)
							AspectUtils.accept(batchReadFuncAspect.state(BatchReadFuncAspect.STATE_ENQUEUED).getEnqueuedConsumers(), tapdataEvents);
					}
				}
		);
		if (admitOutcome == TaskThrottleRuntime.AdmitOutcome.FENCED_OR_STOPPED && throttleRuntime.isFenced()) {
			throw throttleRuntime.buildFencedException("Source partition batch read");
		}
	}

}

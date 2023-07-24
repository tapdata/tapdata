package io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.impl;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryBaseImpl;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryConstant;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustMemoryContext;
import io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory.DynamicAdjustResult;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.lucene.util.RamUsageEstimator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 14:29
 **/
public class DynamicAdjustMemoryImpl extends DynamicAdjustMemoryBaseImpl {

	public DynamicAdjustMemoryImpl(DynamicAdjustMemoryContext context) {
		super(context);
	}

	@Override
	public DynamicAdjustResult calcQueueSize(List<TapEvent> events, int originalQueueSize) {
		if (CollectionUtils.isEmpty(events)) {
			return new DynamicAdjustResult();
		}
		List<TapEvent> sampleList = randomSampleList(events, context.getSampleRate());
		long sizeOfSampleListByte = 0L;
		int sampleCount = 0;
		AtomicReference<String> tableId = new AtomicReference<>();
		for (TapEvent tapEvent : sampleList) {
			if (!(tapEvent instanceof TapRecordEvent)) {
				continue;
			}
			if (null == tableId.get()) {
				tableId.set(((TapRecordEvent) tapEvent).getTableId());
			}
			Map<String, Object> before = TapEventUtil.getBefore(tapEvent);
			Map<String, Object> after = TapEventUtil.getAfter(tapEvent);
			if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
				continue;
			}
			sampleCount++;
			if (MapUtils.isNotEmpty(before)) {
				sizeOfSampleListByte += RamUsageEstimator.sizeOfMap(before);
			}
			if (MapUtils.isNotEmpty(after)) {
				sizeOfSampleListByte += RamUsageEstimator.sizeOfMap(after);
			}
		}
		if (sampleCount <= 0) {
			return new DynamicAdjustResult();
		}
		long finalSizeOfSampleListByte = sizeOfSampleListByte;
		int finalSampleCount = sampleCount;
		long oneEventSizeByte = sizeOfSampleListByte / sampleCount;
		Optional.ofNullable(obsLogger).ifPresent(log -> log.info("{}Sampling table {}'s data, all data size: {}KB, row count: {}, single row data size: {}KB",
				DynamicAdjustMemoryConstant.LOG_PREFIX, tableId.get(), BigDecimal.valueOf(finalSizeOfSampleListByte).divide(new BigDecimal(1024), 2, RoundingMode.HALF_UP), finalSampleCount,
				new BigDecimal(oneEventSizeByte).divide(new BigDecimal(1024), 2, RoundingMode.HALF_UP)));
		if (oneEventSizeByte > context.getRamThreshold()) {
			double coefficient = new BigDecimal(oneEventSizeByte).divide(new BigDecimal(context.getRamThreshold()), 2, RoundingMode.HALF_UP).doubleValue();
			return new DynamicAdjustResult(DynamicAdjustResult.Mode.DECREASE, coefficient);
		} else {
			return new DynamicAdjustResult(DynamicAdjustResult.Mode.INCREASE, 0d);
		}
	}
}

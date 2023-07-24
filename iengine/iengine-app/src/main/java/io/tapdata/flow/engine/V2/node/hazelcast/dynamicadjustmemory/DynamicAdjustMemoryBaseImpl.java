package io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 14:43
 **/
public abstract class DynamicAdjustMemoryBaseImpl implements DynamicAdjustMemoryService {
	public static final int MIN_SAMPLE_SIZE = 10;
	public static final int MAX_SAMPLE_SIZE = 100;
	protected ObsLogger obsLogger;
	protected DynamicAdjustMemoryContext context;

	public DynamicAdjustMemoryBaseImpl(DynamicAdjustMemoryContext context) {
		this.context = context;
		if (null != context.getTaskDto()) {
			this.obsLogger = ObsLoggerFactory.getInstance().getObsLogger(context.getTaskDto().getId().toHexString());
		}
	}

	protected List<TapEvent> randomSampleList(List<TapEvent> events, Double sampleRate) {
		if (CollectionUtils.isEmpty(events)) {
			return null;
		}
		List<TapEvent> copyList = new ArrayList<>(events);
		List<TapEvent> randomSampleList = new ArrayList<>();
		int sampleSize = Math.max(MIN_SAMPLE_SIZE, (int) (copyList.size() * sampleRate));
		sampleSize = Math.min(MAX_SAMPLE_SIZE, sampleSize);
		sampleSize = Math.min(copyList.size(), sampleSize);
		for (int i = 0; i < sampleSize; i++) {
			int randomIndex = RandomUtils.nextInt(0, copyList.size());
			randomSampleList.add(copyList.get(randomIndex));
			copyList.remove(randomIndex);
		}
		Optional.ofNullable(obsLogger).ifPresent(log->{
			if (log.isDebugEnabled()) {
				log.debug("{}Event list size: {}, sample list size: {}, sample rate: {}, min size: {}, max size: {}", DynamicAdjustMemoryConstant.LOG_PREFIX, events.size(), randomSampleList.size(), context.getSampleRate(), MIN_SAMPLE_SIZE, MAX_SAMPLE_SIZE);
			}
		});
		return randomSampleList;
	}
}

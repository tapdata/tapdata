package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.common.sample.sampler.AverageSampler;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.common.sample.sampler.ResetCounterSampler;
import io.tapdata.common.sample.sampler.SpeedSampler;
import io.tapdata.flow.engine.V2.node.hazelcast.HazelcastBaseNode;
import io.tapdata.metrics.TaskSampleRetriever;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-12 17:10
 **/
public abstract class HazelcastProcessorBaseNode extends HazelcastBaseNode {

	// statistic and sample related
	protected ResetCounterSampler resetInputCounter;
	protected CounterSampler inputCounter;
	protected ResetCounterSampler resetOutputCounter;
	protected CounterSampler outputCounter;
	protected SpeedSampler inputQPS;
	protected SpeedSampler outputQPS;
	protected AverageSampler timeCostAvg;

	public HazelcastProcessorBaseNode(ProcessorBaseContext processorBaseContext) {
		super(processorBaseContext);
	}

	@Override
	protected void initSampleCollector() {
		super.initSampleCollector();

		// TODO: init outputCounter initial value
		Map<String, Number> values = TaskSampleRetriever.getInstance().retrieve(tags, Arrays.asList(
				"inputTotal", "outputTotal"
		));
		// init statistic and sample related initialize
		resetInputCounter = statisticCollector.getResetCounterSampler("inputTotal");
		inputCounter = sampleCollector.getCounterSampler("inputTotal", values.getOrDefault("inputTotal", 0).longValue());
		resetOutputCounter = statisticCollector.getResetCounterSampler("outputTotal");
		outputCounter = sampleCollector.getCounterSampler("outputTotal", values.getOrDefault("outputTotal", 0).longValue());
		inputQPS = sampleCollector.getSpeedSampler("inputQPS");
		outputQPS = sampleCollector.getSpeedSampler("outputQPS");
		timeCostAvg = sampleCollector.getAverageSampler("timeCostAvg");

		super.initSampleCollector();
	}

	@Override
	protected final boolean tryProcess(int ordinal, @NotNull Object item) throws Exception {
		TapdataEvent tapdataEvent = (TapdataEvent) item;
		if (null == tapdataEvent.getTapEvent()) {
			while (running.get()) {
				if (offer(tapdataEvent)) break;
			}
			return true;
		}
		// Update memory from ddl event info map
		updateMemoryFromDDLInfoMap(tapdataEvent);
		if (tapdataEvent.isDML()) {
			transformFromTapValue(tapdataEvent, null);
		}
		tryProcess(tapdataEvent, (event, processResult) -> {
			if (null == event) {
				return;
			}
			if (tapdataEvent.isDML()) {
				if (null != processResult && null != processResult.getTableId()) {
					transformToTapValue(event, processorBaseContext.getTapTableMap(), processResult.getTableId());
				} else {
					transformToTapValue(event, processorBaseContext.getTapTableMap(), getNode().getId());
				}
			}
			while (running.get()) {
				if (offer(event)) break;
			}
		});
		return true;
	}

	protected abstract void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer);

	protected static class ProcessResult {
		private String tableId;

		public static ProcessResult create() {
			return new ProcessResult();
		}

		public ProcessResult tableId(String tableId) {
			this.tableId = tableId;
			return this;
		}

		public String getTableId() {
			return tableId;
		}
	}
}

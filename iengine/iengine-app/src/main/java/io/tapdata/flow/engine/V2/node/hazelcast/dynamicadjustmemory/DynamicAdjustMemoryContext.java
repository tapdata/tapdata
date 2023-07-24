package io.tapdata.flow.engine.V2.node.hazelcast.dynamicadjustmemory;

import com.tapdata.tm.commons.task.dto.TaskDto;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2023-07-20 14:35
 **/
public class DynamicAdjustMemoryContext implements Serializable {
	private static final long serialVersionUID = -6496354498498620817L;
	public static final double DEFAULT_SAMPLE_RATE = 0.1D;
	public static final int DEFAULT_MIN_QUEUE_SIZE = 10;
	public static final long DEFAULT_RAM_THRESHOLD_BYTE = 30 * 1024L;
	private double sampleRate = DEFAULT_SAMPLE_RATE;
	private int minQueueSize = DEFAULT_MIN_QUEUE_SIZE;
	private long ramThreshold = DEFAULT_RAM_THRESHOLD_BYTE;
	private TaskDto taskDto;

	private DynamicAdjustMemoryContext() {
	}

	public static DynamicAdjustMemoryContext create() {
		return new DynamicAdjustMemoryContext();
	}

	public DynamicAdjustMemoryContext ramThreshold(long ramThreshold) {
		this.ramThreshold = ramThreshold;
		return this;
	}

	public long getRamThreshold() {
		return ramThreshold;
	}

	public DynamicAdjustMemoryContext sampleRate(Double sampleRate) {
		this.sampleRate = sampleRate;
		return this;
	}

	public Double getSampleRate() {
		return sampleRate;
	}

	public DynamicAdjustMemoryContext taskDto(TaskDto taskDto) {
		this.taskDto = taskDto;
		return this;
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public DynamicAdjustMemoryContext minQueueSize(int minQueueSize) {
		this.minQueueSize = minQueueSize;
		return this;
	}

	public int getMinQueueSize() {
		return minQueueSize;
	}
}

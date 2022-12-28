package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class GetReadPartitionOptions {
	public static final int SPLIT_TYPE_BY_COUNT = 1;
	public static final int SPLIT_TYPE_BY_MINMAX = 10;

	private Long maxRecordInPartition;
	public GetReadPartitionOptions maxRecordInPartition(Long maxRecordInPartition) {
		this.maxRecordInPartition = maxRecordInPartition;
		return this;
	}
	private List<ReadPartition> existingPartitions;
	public GetReadPartitionOptions existingPartitions(List<ReadPartition> existingPartitions) {
		this.existingPartitions = existingPartitions;
		return this;
	}
	private int splitType;
	public GetReadPartitionOptions splitType(int splitType) {
		this.splitType = splitType;
		return this;
	}
	private TypeSplitterMap typeSplitterMap;
	public GetReadPartitionOptions typeSplitterMap(TypeSplitterMap typeSplitterMap) {
		this.typeSplitterMap = typeSplitterMap;
		return this;
	}
	private Consumer<ReadPartition> consumer;
	public GetReadPartitionOptions consumer(Consumer<ReadPartition> consumer) {
		this.consumer = consumer;
		return this;
	}
	private Runnable completedRunnable;
	public GetReadPartitionOptions completedRunnable(Runnable completedRunnable) {
		this.completedRunnable = completedRunnable;
		return this;
	}

	public static GetReadPartitionOptions create() {
		return new GetReadPartitionOptions();
	}

	public Long getMaxRecordInPartition() {
		return maxRecordInPartition;
	}

	public void setMaxRecordInPartition(Long maxRecordInPartition) {
		this.maxRecordInPartition = maxRecordInPartition;
	}

	public List<ReadPartition> getExistingPartitions() {
		return existingPartitions;
	}

	public void setExistingPartitions(List<ReadPartition> existingPartitions) {
		this.existingPartitions = existingPartitions;
	}

	public int getSplitType() {
		return splitType;
	}

	public void setSplitType(int splitType) {
		this.splitType = splitType;
	}

	public TypeSplitterMap getTypeSplitterMap() {
		return typeSplitterMap;
	}

	public void setTypeSplitterMap(TypeSplitterMap typeSplitterMap) {
		this.typeSplitterMap = typeSplitterMap;
	}

	public Consumer<ReadPartition> getConsumer() {
		return consumer;
	}

	public void setConsumer(Consumer<ReadPartition> consumer) {
		this.consumer = consumer;
	}

	public Runnable getCompletedRunnable() {
		return completedRunnable;
	}

	public void setCompletedRunnable(Runnable completedRunnable) {
		this.completedRunnable = completedRunnable;
	}
}

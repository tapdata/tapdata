package io.tapdata.partition;

import io.tapdata.pdk.apis.partition.TapPartitionFilter;

/**
 * @author aplomb
 */
public class SplitProgress {
	private PartitionCollector partitionCollector;
	public SplitProgress partitionCollector(PartitionCollector partitionCollector) {
		this.partitionCollector = partitionCollector;
		return this;
	}
	private long count;
	public SplitProgress count(long count) {
		this.count = count;
		return this;
	}
	private int currentFieldPos;
	public SplitProgress currentFieldPos(int currentFieldPos) {
		this.currentFieldPos = currentFieldPos;
		return this;
	}
	private TapPartitionFilter partitionFilter;
	public SplitProgress partitionFilter(TapPartitionFilter partitionFilter) {
		this.partitionFilter = partitionFilter;
		return this;
	}

	public static SplitProgress create() {
		return new SplitProgress();
	}

	public int getCurrentFieldPos() {
		return currentFieldPos;
	}

	public void setCurrentFieldPos(int currentFieldPos) {
		this.currentFieldPos = currentFieldPos;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public TapPartitionFilter getPartitionFilter() {
		return partitionFilter;
	}

	public void setPartitionFilter(TapPartitionFilter partitionFilter) {
		this.partitionFilter = partitionFilter;
	}

	public PartitionCollector getPartitionCollector() {
		return partitionCollector;
	}

	public void setPartitionCollector(PartitionCollector partitionCollector) {
		this.partitionCollector = partitionCollector;
	}
}

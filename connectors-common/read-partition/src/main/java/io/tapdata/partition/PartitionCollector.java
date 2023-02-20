package io.tapdata.partition;

import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.*;

/**
 * @author aplomb
 */
public class PartitionCollector {
	public static final int STATE_NONE = 1;
	public static final int STATE_MIN_MAX = 10;
	public static final int STATE_SPLIT = 20;
	public static final int STATE_COUNT = 30;
	public static final int STATE_DONE = 40;
	private int state = STATE_NONE;
	public PartitionCollector state(int state) {
		this.state = state;
		return this;
	}

	private PartitionCollector nextSplit;
	public PartitionCollector nextSplit(PartitionCollector nextSplit) {
		this.nextSplit = nextSplit;
		return this;
	}
	private PartitionCollector nextIndex;
	public PartitionCollector nextIndex(PartitionCollector nextIndex) {
		this.nextIndex = nextIndex;
		return this;
	}
//	private List<PartitionCollector> siblings;
	private PartitionCollector sibling;
	public PartitionCollector sibling(PartitionCollector sibling) {
//		if(siblings == null)
//			siblings = new CopyOnWriteArrayList<>();
//		siblings.add(sibling);
		this.sibling = sibling;
		return this;
	}

	private final Map<TapPartitionFilter, Long> partitionCountMap = Collections.synchronizedMap(new LinkedHashMap<>());
	public PartitionCollector addPartition(TapPartitionFilter partitionFilter, Long count) {
		partitionCountMap.put(partitionFilter, count);
		return this;
	}

	public Map<TapPartitionFilter, Long> getPartitionCountMap() {
		return partitionCountMap;
	}

	public PartitionCollector getSibling() {
		return sibling;
	}

	public void setSibling(PartitionCollector sibling) {
		this.sibling = sibling;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public PartitionCollector getNextSplit() {
		return nextSplit;
	}

	public void setNextSplit(PartitionCollector nextSplit) {
		this.nextSplit = nextSplit;
	}

	public PartitionCollector getNextIndex() {
		return nextIndex;
	}

	public void setNextIndex(PartitionCollector nextIndex) {
		this.nextIndex = nextIndex;
	}
}

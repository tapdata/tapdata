package io.tapdata.partition;

import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

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

//	private List<PartitionCollector> siblings;
	private PartitionCollector next;
	public PartitionCollector next(PartitionCollector next) {
//		if(siblings == null)
//			siblings = new CopyOnWriteArrayList<>();
//		siblings.add(sibling);
		this.next = next;
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

	public PartitionCollector getNext() {
		return next;
	}

	public void setNext(PartitionCollector next) {
		this.next = next;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}
}

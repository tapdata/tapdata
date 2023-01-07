package com.tapdata.tm.commons.dag.vo;

import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;

/**
 * @author aplomb
 */
public class ReadPartitionOptions {
	public static final int SPLIT_TYPE_NONE = 0;
	public static final int SPLIT_TYPE_BY_COUNT = GetReadPartitionOptions.SPLIT_TYPE_BY_COUNT;
	public static final int SPLIT_TYPE_BY_MINMAX = GetReadPartitionOptions.SPLIT_TYPE_BY_MINMAX;
	private int splitType = SPLIT_TYPE_BY_COUNT;

	private long maxRecordInPartition = 200_000;
	public int getSplitType() {
		return splitType;
	}

	public void setSplitType(int splitType) {
		this.splitType = splitType;
	}

	public long getMaxRecordInPartition() {
		return maxRecordInPartition;
	}

	public void setMaxRecordInPartition(long maxRecordInPartition) {
		this.maxRecordInPartition = maxRecordInPartition;
	}

	@Override
	public String toString() {
		return "ReadPartitionOptions maxRecordInPartition " + maxRecordInPartition + " splitType " + (splitType == SPLIT_TYPE_BY_COUNT ? "by count" : "by min/max");
	}
}

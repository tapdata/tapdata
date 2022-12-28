package com.tapdata.tm.commons.dag.vo;

import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionsFunction;

/**
 * @author aplomb
 */
public class ReadPartitionOptions {
	public static final int SPLIT_TYPE_NONE = 0;
	public static final int SPLIT_TYPE_BY_COUNT = GetReadPartitionOptions.SPLIT_TYPE_BY_COUNT;
	public static final int SPLIT_TYPE_BY_MINMAX = GetReadPartitionOptions.SPLIT_TYPE_BY_MINMAX;
	private int splitType = SPLIT_TYPE_BY_COUNT;

	private long maxRecordInPartition = 500_000;
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
}

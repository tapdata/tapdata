package com.tapdata.tm.commons.dag.vo;

import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;

import java.io.Serializable;

/**
 * @author aplomb
 */
public class ReadPartitionOptions implements Serializable {
	public static final int SPLIT_TYPE_NONE = 0;
	public static final int SPLIT_TYPE_BY_COUNT = GetReadPartitionOptions.SPLIT_TYPE_BY_COUNT;
	public static final int SPLIT_TYPE_BY_MINMAX = GetReadPartitionOptions.SPLIT_TYPE_BY_MINMAX;
	private int splitType = SPLIT_TYPE_BY_COUNT;

	private boolean enable = false;
	private long maxRecordInPartition = 500_000;
	private int partitionThreadCount = 8;
	private int partitionBatchCount = 3000;
	private int minMaxSplitPieces = 100;
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

	public boolean isEnable() {
		return enable;
	}

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

	public int getPartitionThreadCount() {
		return partitionThreadCount;
	}

	public void setPartitionThreadCount(int partitionThreadCount) {
		this.partitionThreadCount = partitionThreadCount;
	}

	public int getPartitionBatchCount() {
		return partitionBatchCount;
	}

	public void setPartitionBatchCount(int partitionBatchCount) {
		this.partitionBatchCount = partitionBatchCount;
	}

	public int getMinMaxSplitPieces() {
		return minMaxSplitPieces;
	}

	public void setMinMaxSplitPieces(int minMaxSplitPieces) {
		this.minMaxSplitPieces = minMaxSplitPieces;
	}

	@Override
	public String toString() {
		return "ReadPartitionOptions enable " + enable + " maxRecordInPartition " + maxRecordInPartition + " splitType " + (splitType == SPLIT_TYPE_BY_COUNT ? "by count" : "by min/max" + " minMaxSplitPieces " + minMaxSplitPieces + " partitionBatchCount " + partitionBatchCount + " partitionThreadCount " + partitionThreadCount);
	}
}

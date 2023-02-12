package io.tapdata.partition;

import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.List;

/**
 * @author aplomb
 */
public class ReadPartitionCheckPoint {
	private List<ReadPartition> readPartitions;
	private SplitContext splitContext;
	private List<SplitProgress> splitProgresses;
}

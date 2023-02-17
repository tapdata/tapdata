package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.entity.serializer.JavaCustomSerializer;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author aplomb
 */
public class PartitionTableOffset implements Serializable {
	private List<ReadPartition> partitions;
	public PartitionTableOffset partitions(List<ReadPartition> partitions) {
		this.partitions = partitions;
		return this;
	}

	private Map<String, Long> completedPartitions;
	public PartitionTableOffset completedPartitions(Map<String, Long> completedPartitions) {
		this.completedPartitions = completedPartitions;
		return this;
	}

	private String table;
	public PartitionTableOffset table(String table) {
		this.table = table;
		return this;
	}
	private Boolean tableCompleted;
	public PartitionTableOffset tableCompleted(boolean tableCompleted) {
		this.tableCompleted = tableCompleted;
		return this;
	}

	public void partitionCompleted(String partitionId, long total) {
		if(completedPartitions == null)
			completedPartitions = new ConcurrentHashMap<>();
		completedPartitions.put(partitionId, total);
	}

	public List<ReadPartition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<ReadPartition> partitions) {
		this.partitions = partitions;
	}

	public Map<String, Long> getCompletedPartitions() {
		return completedPartitions;
	}

	public void setCompletedPartitions(Map<String, Long> completedPartitions) {
		this.completedPartitions = completedPartitions;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Boolean getTableCompleted() {
		return tableCompleted;
	}

	public void setTableCompleted(Boolean tableCompleted) {
		this.tableCompleted = tableCompleted;
	}

}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author aplomb
 */
public class PartitionTableOffset {
	private List<ReadPartition> partitions;
	public PartitionTableOffset partitions(List<ReadPartition> partitions) {
		this.partitions = partitions;
		return this;
	}

	private Set<String> completedPartitions;
	public PartitionTableOffset completedPartitions(Set<String> completedPartitions) {
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

	public void partitionCompleted(String partitionId) {
		if(completedPartitions == null)
			completedPartitions = new HashSet<>();
		completedPartitions.add(partitionId);
	}

	public List<ReadPartition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<ReadPartition> partitions) {
		this.partitions = partitions;
	}

	public Set<String> getCompletedPartitions() {
		return completedPartitions;
	}

	public void setCompletedPartitions(Set<String> completedPartitions) {
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

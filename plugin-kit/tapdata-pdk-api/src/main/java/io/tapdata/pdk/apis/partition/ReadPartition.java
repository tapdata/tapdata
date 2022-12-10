package io.tapdata.pdk.apis.partition;

/**
 * @author aplomb
 */
public class ReadPartition {
	private String id;
	public ReadPartition id(String id) {
		this.id = id;
		return this;
	}
	private TapPartitionFilter partitionFilter;
	public ReadPartition partitionFilter(TapPartitionFilter partitionFilter) {
		this.partitionFilter = partitionFilter;
		return this;
	}

	public static ReadPartition create() {
		return new ReadPartition();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public TapPartitionFilter getPartitionFilter() {
		return partitionFilter;
	}

	public void setPartitionFilter(TapPartitionFilter partitionFilter) {
		this.partitionFilter = partitionFilter;
	}

	@Override
	public String toString() {
		return "ReadPartition id " + id + " " + partitionFilter;
	}
}

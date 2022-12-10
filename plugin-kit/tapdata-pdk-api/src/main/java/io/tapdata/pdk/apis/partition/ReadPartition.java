package io.tapdata.pdk.apis.partition;

/**
 * @author aplomb
 */
public class ReadPartition {
	public static final int STATE_NONE = 1;
	public static final int STATE_READING = 10;
	public static final int STATE_DONE = 100;
	private int state = STATE_NONE;
	public ReadPartition state(int state) {
		this.state = state;
		return this;
	}
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

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	@Override
	public String toString() {
		return "ReadPartition id " + id + " " + partitionFilter;
	}
}

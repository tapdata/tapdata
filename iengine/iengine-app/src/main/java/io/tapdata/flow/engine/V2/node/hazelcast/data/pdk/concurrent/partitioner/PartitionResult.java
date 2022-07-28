package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner;

/**
 * @author jackin
 * @date 2022/7/26 09:10
 **/
public class PartitionResult<T> {

	private int partition;

	private T event;

	public PartitionResult(int partition, T event) {
		this.partition = partition;
		this.event = event;
	}

	public int getPartition() {
		return partition;
	}

	public T getEvent() {
		return event;
	}
}

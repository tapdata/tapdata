package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner;

/**
 * @author jackin
 * @date 2022/7/25 17:38
 **/
public interface Partitioner<T, V> {

	PartitionResult<T> partition(int partitionSize, T event, V partitionValue);
}

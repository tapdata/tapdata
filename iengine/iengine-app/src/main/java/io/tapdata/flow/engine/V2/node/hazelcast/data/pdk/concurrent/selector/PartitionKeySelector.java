package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector;

import java.util.List;

/**
 * @author jackin
 * @date 2022/7/26 09:23
 **/
public interface PartitionKeySelector<K, T, R> {

	List<R> select(T event);
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.selector;

import java.util.List;

/**
 * @author jackin
 * @date 2022/7/26 09:23
 **/
public interface PartitionKeySelector<T, R, M> {

	List<R> select(T event, M row);

	List<R> convert2OriginValue(final List<R> values);
}

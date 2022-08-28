package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.concurrent.partitioner;

import com.tapdata.entity.TapdataEvent;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;

/**
 * @author jackin
 * @date 2022/7/26 09:19
 **/
public class KeysPartitioner implements Partitioner<TapdataEvent, List<Object>> {

	@Override
	public PartitionResult<TapdataEvent> partition(int partitionSize, TapdataEvent tapdataEvent, List<Object> partitionValue) {

		if (tapdataEvent == null) {
			return null;
		}

		int partition = 0;
		if (CollectionUtils.isNotEmpty(partitionValue)) {
			partition = Math.abs(Objects.hash(partitionValue)) % partitionSize;
		}
		return new PartitionResult<>(partition, tapdataEvent);
	}

}

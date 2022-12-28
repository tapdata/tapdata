package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

/**
 * @author aplomb
 */
public class TapEventPartitionDispatcher {
	private final TapTable table;
	private Collection<String> partitionFields;
	private Map<ReadPartition, Consumer<List<TapEvent>>> readPartitionConsumerMap = new ConcurrentSkipListMap<>(ReadPartition::compareTo);

	public TapEventPartitionDispatcher(TapTable table) {
		this.table = table;
		TapIndexEx partitionIndex = table.partitionIndex();
		if(partitionIndex != null && partitionIndex.getIndexFields() != null) {
			partitionFields = new ArrayList<>();
			for(TapIndexField field : partitionIndex.getIndexFields()) {
				partitionFields.add(field.getName());
			}
		}
	}

	public void register(ReadPartition readPartition, Consumer<List<TapEvent>> consumer) {
		readPartitionConsumerMap.put(readPartition, consumer);
	}

	public void receivedTapEvents(List<TapEvent> events) {

	}
}

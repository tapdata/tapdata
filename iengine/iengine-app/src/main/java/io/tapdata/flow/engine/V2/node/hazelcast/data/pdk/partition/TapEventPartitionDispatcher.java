package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.deleteDMLEvent;

/**
 * @author aplomb
 */
public class TapEventPartitionDispatcher extends PartitionFieldParentHandler {
	private final ConcurrentSkipListMap<ReadPartition, ReadPartitionKVStorage> readPartitionConsumerMap = new ConcurrentSkipListMap<>(/*ReadPartition::compareTo*/);

	private final AtomicBoolean readPartitionFinished = new AtomicBoolean(false);

	private final ObsLogger obsLogger;
	public TapEventPartitionDispatcher(TapTable tapTable, ObsLogger obsLogger) {
		super(tapTable);

		this.obsLogger = obsLogger;
		typeHandlers.register(TapInsertRecordEvent.class, this::handleInsertRecordEvent);
		typeHandlers.register(TapUpdateRecordEvent.class, this::handleUpdateRecordEvent);
		typeHandlers.register(TapDeleteRecordEvent.class, this::handleDeleteRecordEvent);
		typeHandlers.register(TapDDLEvent.class, this::handleDDLEvent);
		typeHandlers.register(ControlEvent.class, this::handleControlEvent);
	}

	private Void handleControlEvent(ControlEvent controlEvent) {
		throw new CoreException(PartitionErrorCodes.CONTROL_NOT_ALLOWED_IN_PARTITION, "Control event {} not allowed during partition", controlEvent);
	}
	private Void handleDDLEvent(TapDDLEvent ddlEvent) {
		throw new CoreException(PartitionErrorCodes.DDL_NOT_ALLOWED_IN_PARTITION, "DDL event {} not allowed during partition", ddlEvent);
	}

	private Void handleInsertRecordEvent(TapInsertRecordEvent insertRecordEvent) {
		Map<String, Object> after = reviseData(insertRecordEvent.getAfter());
		Map<String, Object> key = getKeyFromData(after);
		ReadPartition readPartition = readPartitionConsumerMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match(key)));
		if(readPartition == null) {
//			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "Partition not found for key {}", key);
			obsLogger.warn("Partition not found for key {}, insertRecord will be ignored {} readPartitionConsumerMap size {}", key, insertRecordEvent.getAfter(), readPartitionConsumerMap.size());
			return null;
		}
		obsLogger.info("Table {} InsertRecord key {} assigned into partition {}", table, key, readPartition);
		ReadPartitionKVStorage readPartitionHandler = readPartitionConsumerMap.get(readPartition);
		if(readPartitionHandler == null) {
			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "ReadPartition {} failed to find readPartitionHandler for key {} while insert", readPartition, key);
		}
        readPartitionHandler.handleInsertRecordEvent(insertRecordEvent, after, key);
		return null;
	}

	private Void handleUpdateRecordEvent(TapUpdateRecordEvent updateRecordEvent) {
		Map<String, Object> before = reviseData(updateRecordEvent.getBefore());
		Map<String, Object> after = reviseData(updateRecordEvent.getAfter());
		Map<String, Object> key = getKeyFromData(before, after);

		ReadPartition readPartition = readPartitionConsumerMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match(key)));
		if(readPartition == null) {
//			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "Partition not found for value {}", key);
			obsLogger.warn("Partition not found for value {}, updateRecord will be ignored before {} after {}, readPartitionConsumerMap size {}", key, updateRecordEvent.getBefore(), updateRecordEvent.getAfter(), readPartitionConsumerMap.size());
			return null;
		}
		obsLogger.info("Table {} UpdateRecord key {} assigned into partition {}", table, key, readPartition);
		ReadPartitionKVStorage readPartitionHandler = readPartitionConsumerMap.get(readPartition);
		if(readPartitionHandler == null) {
			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "ReadPartition {} failed to find readPartitionHandler for key {} while update", readPartition, key);
		}
        readPartitionHandler.handleUpdateRecordEvent(updateRecordEvent, after, key);
		if(!checkKeyChanged(before, after)) {
			obsLogger.info("Partition key has changed in UpdateRecordEvent {} for table {}, will remove the old key from partition. ", updateRecordEvent, table);
			deleteFromPartition(deleteDMLEvent(before, table));
		}
		return null;
	}

	private Void handleDeleteRecordEvent(TapDeleteRecordEvent deleteRecordEvent) {
		Map<String, Object> before = deleteRecordEvent.getBefore();
		Map<String, Object> key = getKeyFromData(before);
		ReadPartition readPartition = readPartitionConsumerMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match(key)));
		if(readPartition == null) {
//			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "Partition not found for value {}", key);
			obsLogger.warn("Partition not found for value {}, deleteRecord will be ignored {}, readPartitionConsumerMap size {}", key, deleteRecordEvent.getBefore(), readPartitionConsumerMap.size());
			return null;
		}
		obsLogger.info("Table {} DeleteRecord key {} assigned into partition {}", table, key, readPartition);
		ReadPartitionKVStorage readPartitionHandler = readPartitionConsumerMap.get(readPartition);
		if(readPartitionHandler == null) {
			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "ReadPartition {} failed to find readPartitionHandler for key {} while delete", readPartition, key);
		}
        readPartitionHandler.handleDeleteRecordEvent(deleteRecordEvent, key);
		return null;
	}

	private void deleteFromPartition(TapDeleteRecordEvent deleteRecordEvent) {
		Map<String, Object> before = deleteRecordEvent.getBefore();
		Map<String, Object> key = getKeyFromData(before);
		ReadPartition readPartition = readPartitionConsumerMap.ceilingKey(ReadPartition.create().partitionFilter(TapPartitionFilter.create().match(key)));
		if(readPartition == null) {
//			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "Partition not found for value {}", key);
			obsLogger.warn("Partition not found for value {}, deleteFromPartition will be ignored {}, readPartitionConsumerMap size {}", key, before, readPartitionConsumerMap.size());
			return;
		}
		obsLogger.info("Table {} deleteFromPartition key {} assigned into partition {}", table, key, readPartition);
		ReadPartitionKVStorage readPartitionHandler = readPartitionConsumerMap.get(readPartition);
		if(readPartitionHandler == null) {
			throw new CoreException(PartitionErrorCodes.PARTITION_NOT_FOUND_FOR_VALUE, "ReadPartition {} failed to find readPartitionHandler for key {} while delete", readPartition, key);
		}
        readPartitionHandler.deleteFromPartition(deleteRecordEvent,key);
	}

	public void register(ReadPartition readPartition, ReadPartitionKVStorage readPartitionHandler) {
		readPartitionConsumerMap.put(readPartition, readPartitionHandler);
	}

	public void receivedTapEvents(List<TapEvent> events) {
		for (TapEvent event : events) {
			typeHandlers.handle(event);
		}
	}

	public void readPartitionFinished() {
		readPartitionFinished.set(true);
	}

	public boolean isReadPartitionFinished() {
		return readPartitionFinished.get();
	}

	public ConcurrentSkipListMap<ReadPartition, ReadPartitionKVStorage> getReadPartitionConsumerMap() {
		return readPartitionConsumerMap;
	}
}

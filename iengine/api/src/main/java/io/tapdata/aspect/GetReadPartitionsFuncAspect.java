package io.tapdata.aspect;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionsFunction;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class GetReadPartitionsFuncAspect extends DataFunctionAspect<GetReadPartitionsFuncAspect> {
	private static final String TAG = GetReadPartitionsFuncAspect.class.getSimpleName();
	private TapConnectorContext connectorContext;

	public static final int STATE_ENQUEUED = 10;

	public static final int STATE_READ_COMPLETE = 11;
	public static final int STATE_PROCESS_COMPLETE = 12;

	public GetReadPartitionsFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	private TapTable table;

	public GetReadPartitionsFuncAspect table(TapTable table) {
		this.table = table;
		return this;
	}
	private int splitType;
	public GetReadPartitionsFuncAspect splitType(int splitType) {
		this.splitType = splitType;
		return this;
	}

	private long maxRecordInPartition = 500_000;
	public GetReadPartitionsFuncAspect maxRecordInPartition(long maxRecordInPartition) {
		this.maxRecordInPartition = maxRecordInPartition;
		return this;
	}
	private List<Consumer<ReadPartition>> enqueuedConsumers = null;
	public GetReadPartitionsFuncAspect enqueuedConsumer(Consumer<ReadPartition> listConsumer) {
		if (null == enqueuedConsumers) {
			enqueuedConsumers = new CopyOnWriteArrayList<>();
		}
		this.enqueuedConsumers.add(readPartition -> {
			try {
				listConsumer.accept(readPartition);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume tapdataEvents from table {} failed on enqueued consumer {}, {}",
						table, listConsumer, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			}
		});
		return this;
	}

	private List<Consumer<ReadPartition>> readCompleteConsumers = null;
	public GetReadPartitionsFuncAspect readCompleteConsumer(Consumer<ReadPartition> listConsumer) {
		if (null == readCompleteConsumers) {
			readCompleteConsumers = new CopyOnWriteArrayList<>();
		}
		this.readCompleteConsumers.add(readPartition -> {
			try {
				listConsumer.accept(readPartition);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume readPartition from table {} failed on read complete consumer {}, {}",
						table, listConsumer, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			}
		});
		return this;
	}

	private List<Consumer<ReadPartition>> processCompleteConsumers = null;
	public GetReadPartitionsFuncAspect processCompleteConsumer(Consumer<ReadPartition> listConsumer) {
		if (null == processCompleteConsumers) {
			processCompleteConsumers = new CopyOnWriteArrayList<>();
		}
		this.processCompleteConsumers.add(readPartition -> {
			try {
				listConsumer.accept(readPartition);
			} catch(Throwable throwable) {
				TapLogger.warn(TAG, "Consume readPartition from table {} failed on read complete consumer {}, {}",
						table, listConsumer, InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
			}
		});
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapTable getTable() {
		return table;
	}

	public void setTable(TapTable table) {
		this.table = table;
	}

	public List<Consumer<ReadPartition>> getEnqueuedConsumers() {
		return enqueuedConsumers;
	}

	public void setEnqueuedConsumers(List<Consumer<ReadPartition>> enqueuedConsumers) {
		this.enqueuedConsumers = enqueuedConsumers;
	}

	public List<Consumer<ReadPartition>> getReadCompleteConsumers() {
		return readCompleteConsumers;
	}

	public void setReadCompleteConsumers(List<Consumer<ReadPartition>> readCompleteConsumers) {
		this.readCompleteConsumers = readCompleteConsumers;
	}

	public List<Consumer<ReadPartition>> getProcessCompleteConsumers() {
		return processCompleteConsumers;
	}

	public void setProcessCompleteConsumers(List<Consumer<ReadPartition>> processCompleteConsumers) {
		this.processCompleteConsumers = processCompleteConsumers;
	}

	public int getSplitType() {
		return splitType;
	}

	public void setSplitType(int splitType) {
		this.splitType = splitType;
	}

	public long getMaxRecordInPartition() {
		return maxRecordInPartition;
	}

	public void setMaxRecordInPartition(long maxRecordInPartition) {
		this.maxRecordInPartition = maxRecordInPartition;
	}
}

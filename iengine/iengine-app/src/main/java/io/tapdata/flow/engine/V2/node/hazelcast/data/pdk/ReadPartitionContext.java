package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.partition.ReadPartition;

/**
 * @author aplomb
 */
public class ReadPartitionContext {
	private TapTable table;
	public ReadPartitionContext table(TapTable table) {
		this.table = table;
		return this;
	}
	private PDKSourceContext pdkSourceContext;

	public static ReadPartitionContext create() {
		return new ReadPartitionContext();
	}

	public ReadPartitionContext pdkSourceContext(PDKSourceContext pdkSourceContext) {
		this.pdkSourceContext = pdkSourceContext;
		return this;
	}

	private ReadPartition readPartition;
	public ReadPartitionContext readPartition(ReadPartition readPartition) {
		this.readPartition = readPartition;
		return this;
	}

	private BatchReadFuncAspect batchReadFuncAspect;
	public ReadPartitionContext batchReadFuncAspect(BatchReadFuncAspect batchReadFuncAspect) {
		this.batchReadFuncAspect = batchReadFuncAspect;
		return this;
	}

	public ReadPartition getReadPartition() {
		return readPartition;
	}

	public void setReadPartition(ReadPartition readPartition) {
		this.readPartition = readPartition;
	}

	public PDKSourceContext getPdkSourceContext() {
		return pdkSourceContext;
	}

	public void setPdkSourceContext(PDKSourceContext pdkSourceContext) {
		this.pdkSourceContext = pdkSourceContext;
	}

	public TapTable getTable() {
		return table;
	}

	public void setTable(TapTable table) {
		this.table = table;
	}

	public BatchReadFuncAspect getBatchReadFuncAspect() {
		return batchReadFuncAspect;
	}

	public void setBatchReadFuncAspect(BatchReadFuncAspect batchReadFuncAspect) {
		this.batchReadFuncAspect = batchReadFuncAspect;
	}
}

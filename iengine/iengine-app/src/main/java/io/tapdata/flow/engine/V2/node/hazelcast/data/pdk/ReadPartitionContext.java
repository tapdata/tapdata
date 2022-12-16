package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import io.tapdata.pdk.apis.partition.ReadPartition;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author aplomb
 */
public class ReadPartitionContext {
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
}

package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author aplomb
 */
public class PDKSourceContext {
	private HazelcastSourcePartitionReadDataNode sourcePdkDataNode;

	public static PDKSourceContext create() {
		return new PDKSourceContext();
	}

	public PDKSourceContext sourcePdkDataNode(HazelcastSourcePartitionReadDataNode sourcePdkDataNode) {
		this.sourcePdkDataNode = sourcePdkDataNode;
		return this;
	}
	private List<String> pendingInitialSyncTables;
	public PDKSourceContext pendingInitialSyncTables(List<String> pendingInitialSyncTables) {
		if(pendingInitialSyncTables != null)
			this.pendingInitialSyncTables = new CopyOnWriteArrayList<>(pendingInitialSyncTables);
		else
			this.pendingInitialSyncTables = new CopyOnWriteArrayList<>();
		return this;
	}
	private boolean needCDC;
	public PDKSourceContext needCDC(boolean needCDC) {
		this.needCDC = needCDC;
		return this;
	}

	public HazelcastSourcePartitionReadDataNode getSourcePdkDataNode() {
		return sourcePdkDataNode;
	}

	public boolean isNeedCDC() {
		return needCDC;
	}

	public boolean isNeedInitialSync() {
		return pendingInitialSyncTables != null && !pendingInitialSyncTables.isEmpty();
	}

	public List<String> getPendingInitialSyncTables() {
		return pendingInitialSyncTables;
	}
}

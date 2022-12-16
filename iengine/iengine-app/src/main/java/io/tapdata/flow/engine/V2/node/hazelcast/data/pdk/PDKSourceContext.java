package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author aplomb
 */
public class PDKSourceContext {
	private HazelcastSourcePdkDataNodeEx sourcePdkDataNode;

	public static PDKSourceContext create() {
		return new PDKSourceContext();
	}

	public PDKSourceContext sourcePdkDataNode(HazelcastSourcePdkDataNodeEx sourcePdkDataNode) {
		this.sourcePdkDataNode = sourcePdkDataNode;
		return this;
	}
	private List<String> pendingInitialSyncTables;
	public PDKSourceContext pendingInitialSyncTables(List<String> pendingInitialSyncTables) {
		this.pendingInitialSyncTables = new CopyOnWriteArrayList<>(pendingInitialSyncTables);
		return this;
	}
	private boolean needCDC;
	public PDKSourceContext needCDC(boolean needCDC) {
		this.needCDC = needCDC;
		return this;
	}

	public HazelcastSourcePdkDataNodeEx getSourcePdkDataNode() {
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

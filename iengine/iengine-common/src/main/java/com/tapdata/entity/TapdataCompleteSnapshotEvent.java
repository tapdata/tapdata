package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2022-05-19 16:06
 **/
public class TapdataCompleteSnapshotEvent extends TapdataEvent {
	private static final long serialVersionUID = 5717006177280281597L;
	private String nodeName;
	private String nodeId;

	public TapdataCompleteSnapshotEvent() {
		this.syncStage = SyncStage.INITIAL_SYNC;
	}

	public TapdataCompleteSnapshotEvent(String nodeName, String nodeId) {
		this.nodeName = nodeName;
		this.nodeId = nodeId;
		this.syncStage = SyncStage.INITIAL_SYNC;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
}

package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataNodeAspect;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/5 00:30 Create
 */
public class SnapshotWriteTableCompleteAspect extends DataNodeAspect<SnapshotWriteTableCompleteAspect> {

	private String sourceNodeId;
	private String sourceTableName;

	public SnapshotWriteTableCompleteAspect sourceNodeId(String sourceNodeId) {
		this.sourceNodeId = sourceNodeId;
		return this;
	}

	public SnapshotWriteTableCompleteAspect sourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
		return this;
	}

	public String getSourceNodeId() {
		return sourceNodeId;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}
}

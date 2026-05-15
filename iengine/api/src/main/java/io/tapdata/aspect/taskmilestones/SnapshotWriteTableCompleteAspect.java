package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataNodeAspect;
import lombok.Getter;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/5 00:30 Create
 */
@Getter
public class SnapshotWriteTableCompleteAspect extends DataNodeAspect<SnapshotWriteTableCompleteAspect> {

    private String sourceNodeId;
    private String sourceTableName;
	private boolean errorSkipped;

	public SnapshotWriteTableCompleteAspect sourceNodeId(String sourceNodeId) {
		this.sourceNodeId = sourceNodeId;
		return this;
	}

	public SnapshotWriteTableCompleteAspect sourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
		return this;
	}

	public SnapshotWriteTableCompleteAspect errorSkipped(boolean errorSkipped) {
        this.errorSkipped = errorSkipped;
        return this;
    }
}

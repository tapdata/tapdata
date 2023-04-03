package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataNodeAspect;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/5 00:15 Create
 */
public class SnapshotReadTableBeginAspect extends DataNodeAspect<SnapshotReadTableBeginAspect> {

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public SnapshotReadTableBeginAspect tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
}

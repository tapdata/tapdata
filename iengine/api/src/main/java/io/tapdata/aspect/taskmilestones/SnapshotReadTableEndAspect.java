package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataNodeAspect;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/5 00:15 Create
 */
public class SnapshotReadTableEndAspect extends DataNodeAspect<SnapshotReadTableEndAspect> {

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public SnapshotReadTableEndAspect tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }
}

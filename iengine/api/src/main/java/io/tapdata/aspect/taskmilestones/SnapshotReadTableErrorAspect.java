package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.AbsDataNodeErrorAspect;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/5 00:15 Create
 */
public class SnapshotReadTableErrorAspect extends AbsDataNodeErrorAspect<SnapshotReadTableErrorAspect> {

    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public SnapshotReadTableErrorAspect tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

}

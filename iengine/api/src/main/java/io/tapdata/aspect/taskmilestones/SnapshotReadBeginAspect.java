package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataNodeAspect;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/3/3 10:47 Create
 */
public class SnapshotReadBeginAspect extends DataNodeAspect<SnapshotReadBeginAspect> {

    private List<String> tables;

    public List<String> getTables() {
        return tables;
    }

    public SnapshotReadBeginAspect tables(List<String> tables) {
        this.tables = tables;
        return this;
    }
}

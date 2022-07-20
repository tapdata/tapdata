package io.tapdata.dummy.po;

import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.dummy.constants.SyncStage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy offset.
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/6/22 16:53 Create
 */
public class DummyOffset implements Serializable {

    private SyncStage syncStage;
    private long beginTimes;
    private long lastTimes;
    private long lastTN;
    private Map<String, DummyOffsetStats> tableStats = new HashMap<>();

    public SyncStage getSyncStage() {
        return syncStage;
    }

    public void setSyncStage(SyncStage syncStage) {
        this.syncStage = syncStage;
    }

    public long getBeginTimes() {
        return beginTimes;
    }

    public void setBeginTimes(long beginTimes) {
        this.beginTimes = beginTimes;
    }

    public long getLastTimes() {
        return lastTimes;
    }

    public void setLastTimes(long lastTimes) {
        this.lastTimes = lastTimes;
    }

    public long getLastTN() {
        return lastTN;
    }

    public void setLastTN(long lastTN) {
        this.lastTN = lastTN;
    }

    public Map<String, DummyOffsetStats> getTableStats() {
        return tableStats;
    }

    public void setTableStats(Map<String, DummyOffsetStats> tableStats) {
        this.tableStats = tableStats;
    }

    /**
     * Add and counts records stats
     *
     * @param op        record operator
     * @param tableName table name
     * @param val       add size
     * @return the number of corresponding operation records
     */
    public long addCounts(RecordOperators op, String tableName, int val) {
        long totals = 0;
        DummyOffsetStats dummyOffsetStats = tableStats.computeIfAbsent(tableName, s -> new DummyOffsetStats());
        switch (op) {
            case Insert:
                totals = dummyOffsetStats.getInsertTotals() + val;
                dummyOffsetStats.setInsertTotals(totals);
                return totals;
            case Update:
                totals = dummyOffsetStats.getUpdateTotals() + val;
                dummyOffsetStats.setUpdateTotals(totals);
                return totals;
            case Delete:
                totals = dummyOffsetStats.getDeleteTotals() + val;
                dummyOffsetStats.setDeleteTotals(totals);
                return totals;
        }
        return totals;
    }
}

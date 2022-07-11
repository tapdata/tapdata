package io.tapdata.dummy.po;

import java.io.Serializable;

/**
 * Dummy offset stats.
 * count the number of records inserted, modified and deleted
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/6/22 18:43 Create
 */
public class DummyOffsetStats implements Serializable {
    private long insertTotals;
    private long updateTotals;
    private long deleteTotals;

    public long getInsertTotals() {
        return insertTotals;
    }

    public void setInsertTotals(long insertTotals) {
        this.insertTotals = insertTotals;
    }

    public long getUpdateTotals() {
        return updateTotals;
    }

    public void setUpdateTotals(long updateTotals) {
        this.updateTotals = updateTotals;
    }

    public long getDeleteTotals() {
        return deleteTotals;
    }

    public void setDeleteTotals(long deleteTotals) {
        this.deleteTotals = deleteTotals;
    }
}

package io.tapdata.connector.dameng.cdc.logminer.bean;

import io.tapdata.kit.EmptyKit;

import java.util.Collections;
import java.util.List;

public class DamengRedoLogBatch {

    /**
     * key: instance thread
     * value: redo logs list
     */
    private List<RedoLog> redoLogList;

    private boolean isOnlineRedo;

    public DamengRedoLogBatch(List<RedoLog> redoLogList, boolean isOnlineRedo) {
        if (EmptyKit.isNotEmpty(redoLogList)) {
                if (EmptyKit.isNotEmpty(redoLogList)) {
                    Collections.sort(redoLogList);
                }
            }
        this.redoLogList = redoLogList;
        this.isOnlineRedo = isOnlineRedo;
    }

    public List<RedoLog> getRedoLogList() {
        return redoLogList;
    }

    public void setRedoLogMap(List<RedoLog> redoLogList) {
        this.redoLogList = redoLogList;
    }

    public boolean isOnlineRedo() {
        return isOnlineRedo;
    }

    public void setOnlineRedo(boolean onlineRedo) {
        isOnlineRedo = onlineRedo;
    }


    @Override
    public String toString() {
        return "OracleRedoLogBatch{" + "redoLogList=" + redoLogList +
                ", isOnlineRedo=" + isOnlineRedo +
                '}';
    }
}

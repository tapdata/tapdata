package io.tapdata.pdk.core.workflow.engine;

import java.util.List;

public class JobOptions {
    protected int eventBatchSize = 1000;
    public JobOptions eventBatchSize(int eventBatchSize) {
        this.eventBatchSize = eventBatchSize;
        return this;
    }

    protected int queueSize = 20;
    public JobOptions queueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }
    protected int queueBatchSize = 10;
    public JobOptions queueBatchSize(int queueBatchSize) {
        this.queueBatchSize = queueBatchSize;
        return this;
    }
    public static final String ACTION_DROP_TABLE = "dropTable";
    public static final String ACTION_CREATE_TABLE = "createTable";
    public static final String ACTION_CLEAR_TABLE = "clearTable";
    public static final String ACTION_INDEX_PRIMARY = "indexPrimary";
    protected List<String> actionsBeforeStart;
    public JobOptions actionsBeforeStart(List<String> actionsBeforeStart) {
        this.actionsBeforeStart = actionsBeforeStart;
        return this;
    }

    protected boolean enableBatchRead = true;
    public JobOptions enableBatchRead(boolean enableBatchRead) {
        this.enableBatchRead = enableBatchRead;
        return this;
    }

    protected boolean enableStreamRead = true;
    public JobOptions enableStreamRead(boolean enableStreamRead) {
        this.enableStreamRead = enableStreamRead;
        return this;
    }

    public boolean isEnableBatchRead() {
        return enableBatchRead;
    }

    public void setEnableBatchRead(boolean enableBatchRead) {
        this.enableBatchRead = enableBatchRead;
    }

    public boolean isEnableStreamRead() {
        return enableStreamRead;
    }

    public void setEnableStreamRead(boolean enableStreamRead) {
        this.enableStreamRead = enableStreamRead;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getQueueBatchSize() {
        return queueBatchSize;
    }

    public void setQueueBatchSize(int queueBatchSize) {
        this.queueBatchSize = queueBatchSize;
    }

    public List<String> getActionsBeforeStart() {
        return actionsBeforeStart;
    }

    public void setActionsBeforeStart(List<String> actionsBeforeStart) {
        this.actionsBeforeStart = actionsBeforeStart;
    }

    public int getEventBatchSize() {
        return eventBatchSize;
    }

    public void setEventBatchSize(int eventBatchSize) {
        this.eventBatchSize = eventBatchSize;
    }
}

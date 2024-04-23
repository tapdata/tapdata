package io.tapdata.milestone.constants;

public enum SyncStatus {

    NORMAL(""),

    TASK_INIT(SyncStatusTag.TASK_INIT_STATUS),

    TABLE_INIT_START(SyncStatusTag.TASK_INIT_STATUS),
    TABLE_INIT(SyncStatusTag.TASK_INIT_STATUS),
    TABLE_INIT_COMPLETE(SyncStatusTag.TASK_INIT_STATUS),
    TABLE_INIT_FAILED(SyncStatusTag.TASK_INIT_STATUS),

    DATA_NODE_INIT(SyncStatusTag.TASK_INIT_STATUS),
    DATA_NODE_INIT_COMPLETED(SyncStatusTag.TASK_INIT_STATUS),

    PROCESS_NODE_INIT(SyncStatusTag.TASK_INIT_STATUS),
    PROCESS_NODE_INIT_COMPLETED(SyncStatusTag.TASK_INIT_STATUS),

    SNAPSHOT_INIT(SyncStatusTag.DO_SNAPSHOT_STATUS),
    DO_SNAPSHOT(SyncStatusTag.DO_SNAPSHOT_STATUS),
    SNAPSHOT_COMPLETED(SyncStatusTag.DO_SNAPSHOT_STATUS),
    SNAPSHOT_FAILED("snapshot_failed"),

    CDC_INIT(SyncStatusTag.DO_CDC_STATUS),
    DO_CDC(SyncStatusTag.DO_CDC_STATUS),
    CDC_FAILED("cdc_failed");
    String type;

    SyncStatus(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}

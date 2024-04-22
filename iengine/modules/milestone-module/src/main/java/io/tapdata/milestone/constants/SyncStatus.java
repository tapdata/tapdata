package io.tapdata.milestone.constants;

public enum SyncStatus {
    NORMAL(""),

    TASK_INIT("task_init"),

    TABLE_INIT_START("task_init"),
    TABLE_INIT("task_init"),
    TABLE_INIT_COMPLETE("task_init"),
    TABLE_INIT_FAILED("task_init"),

    DATA_NODE_INIT("task_init"),
    DATA_NODE_INIT_COMPLETED("task_init"),

    PROCESS_NODE_INIT("task_init"),
    PROCESS_NODE_INIT_COMPLETED("task_init"),

    SNAPSHOT_INIT("do_snapshot"),
    DO_SNAPSHOT("do_snapshot"),
    SNAPSHOT_COMPLETED("snapshot_completed"),
    SNAPSHOT_FAILED("snapshot_failed"),

    CDC_INIT("do_cdc"),
    DO_CDC("do_cdc"),
    CDC_FAILED("cdc_failed");
    String type;

    SyncStatus(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}

package io.tapdata.bigquery.entity;

public class ContextConfig {
    public static final String TEMP_CURSOR_SCHEMA_NAME = "tempCursorSchema";
    public static final String TEMP_CURSOR_SCHEMA_NAME_DEFAULT = "temp_table";
    public static final Long MERGE_DELAY_DEFAULT = 60 * 60L;
    private String serviceAccount;
    private String projectId;
    private String tableSet;

    private String writeMode;
    private String cursorSchema;
    private String tempCursorSchema;
    private Long mergeDelay;
    private int maxStreamAppendCount = 50000;

    public static ContextConfig create() {
        return new ContextConfig();
    }

    public ContextConfig serviceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
        return this;
    }

    public String serviceAccount() {
        return this.serviceAccount;
    }

    public ContextConfig projectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public String projectId() {
        return this.projectId;
    }

    public ContextConfig tableSet(String tableSet) {
        this.tableSet = tableSet;
        return this;
    }

    public String tableSet() {
        return this.tableSet;
    }

    public ContextConfig mergeDelay(Long mergeDelay) {
        this.mergeDelay = mergeDelay;
        return this;
    }

    public Long mergeDelay() {
        return this.mergeDelay;
    }

    public ContextConfig cursorSchema(String cursorSchema) {
        this.cursorSchema = cursorSchema;
        return this;
    }

    public ContextConfig tempCursorSchema(String tempCursorSchema) {
        this.tempCursorSchema = tempCursorSchema;
        return this;
    }

    public String cursorSchema() {
        return this.cursorSchema;
    }

    public String tempCursorSchema() {
        return this.tempCursorSchema;
    }

    public ContextConfig writeMode(String writeMode) {
        this.writeMode = writeMode;
        return this;
    }

    public String writeMode() {
        return this.writeMode;
    }

    public boolean isMixedUpdates() {
        return "MIXED_UPDATES".equals(this.writeMode);
    }

    public int maxStreamAppendCount() {
        return this.maxStreamAppendCount;
    }

    public ContextConfig maxStreamAppendCount(int maxStreamAppendCount) {
        this.maxStreamAppendCount = maxStreamAppendCount;
        return this;
    }
}

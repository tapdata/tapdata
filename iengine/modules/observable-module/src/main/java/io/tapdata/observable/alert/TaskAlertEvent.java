package io.tapdata.observable.alert;

import io.tapdata.entity.logger.alert.TapAlertType;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Internal engine event projected from a structured {@code alert()} call.
 * Does not carry throwable or full stack; those stay in task logs.
 */
public final class TaskAlertEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String eventId;
    private final TapAlertType type;
    private final String code;
    private final String dedupKey;
    private final String taskId;
    private final String taskRecordId;
    private final String taskName;
    private final String nodeId;
    private final String nodeName;
    private final String agentId;
    private final String message;
    private final String errorCode;
    private final long occurredAt;
    private final Map<String, String> attributes;
    private final int occurrenceCount;

    private TaskAlertEvent(Builder builder) {
        this.eventId = builder.eventId;
        this.type = builder.type;
        this.code = builder.code;
        this.dedupKey = builder.dedupKey;
        this.taskId = builder.taskId;
        this.taskRecordId = builder.taskRecordId;
        this.taskName = builder.taskName;
        this.nodeId = builder.nodeId;
        this.nodeName = builder.nodeName;
        this.agentId = builder.agentId;
        this.message = builder.message;
        this.errorCode = builder.errorCode;
        this.occurredAt = builder.occurredAt;
        this.attributes = builder.attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(builder.attributes);
        this.occurrenceCount = builder.occurrenceCount <= 0 ? 1 : builder.occurrenceCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getEventId() {
        return eventId;
    }

    public TapAlertType getType() {
        return type;
    }

    public String getCode() {
        return code;
    }

    public String getDedupKey() {
        return dedupKey;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskRecordId() {
        return taskRecordId;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getMessage() {
        return message;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public long getOccurredAt() {
        return occurredAt;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public int getOccurrenceCount() {
        return occurrenceCount;
    }

    public String coalesceKey() {
        return String.join("|",
                nullToEmpty(taskId),
                nullToEmpty(nodeId),
                type == null ? "" : type.name(),
                nullToEmpty(code),
                nullToEmpty(dedupKey));
    }

    public TaskAlertEvent withOccurrenceCount(int count) {
        return builder()
                .eventId(eventId)
                .type(type)
                .code(code)
                .dedupKey(dedupKey)
                .taskId(taskId)
                .taskRecordId(taskRecordId)
                .taskName(taskName)
                .nodeId(nodeId)
                .nodeName(nodeName)
                .agentId(agentId)
                .message(message)
                .errorCode(errorCode)
                .occurredAt(occurredAt)
                .attributes(attributes)
                .occurrenceCount(count)
                .build();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    public static final class Builder {
        private String eventId;
        private TapAlertType type;
        private String code;
        private String dedupKey;
        private String taskId;
        private String taskRecordId;
        private String taskName;
        private String nodeId;
        private String nodeName;
        private String agentId;
        private String message;
        private String errorCode;
        private long occurredAt;
        private Map<String, String> attributes;
        private int occurrenceCount = 1;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder type(TapAlertType type) {
            this.type = type;
            return this;
        }

        public Builder code(String code) {
            this.code = code;
            return this;
        }

        public Builder dedupKey(String dedupKey) {
            this.dedupKey = dedupKey;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder taskRecordId(String taskRecordId) {
            this.taskRecordId = taskRecordId;
            return this;
        }

        public Builder taskName(String taskName) {
            this.taskName = taskName;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder nodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public Builder agentId(String agentId) {
            this.agentId = agentId;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder errorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public Builder occurredAt(long occurredAt) {
            this.occurredAt = occurredAt;
            return this;
        }

        public Builder attributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder occurrenceCount(int occurrenceCount) {
            this.occurrenceCount = occurrenceCount;
            return this;
        }

        public TaskAlertEvent build() {
            return new TaskAlertEvent(this);
        }
    }
}

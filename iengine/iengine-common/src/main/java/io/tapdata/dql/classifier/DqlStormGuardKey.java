package io.tapdata.dql.classifier;

import java.text.Normalizer;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Stable dimensions used to isolate unknown-error windows.
 */
public final class DqlStormGuardKey {
    private static final int MAX_NORMALIZED_MESSAGE_LENGTH = 256;
    private static final String UNKNOWN_DIMENSION = "<unknown>";
    private static final Pattern UUID = Pattern.compile(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern IPV4 = Pattern.compile(
            "\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b");
    private static final Pattern HEX = Pattern.compile("\\b0x[0-9a-f]+\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern NUMBER = Pattern.compile("\\b\\d+\\b");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    private final String taskId;
    private final String failedNodeId;
    private final String tableId;
    private final String errorCode;
    private final String normalizedErrorMessage;

    private DqlStormGuardKey(String taskId,
                             String failedNodeId,
                             String tableId,
                             String errorCode,
                             String normalizedErrorMessage) {
        this.taskId = normalizeDimension(taskId);
        this.failedNodeId = normalizeDimension(failedNodeId);
        this.tableId = normalizeDimension(tableId);
        this.errorCode = normalizeDimension(errorCode);
        this.normalizedErrorMessage = normalizedErrorMessage;
    }

    public static DqlStormGuardKey of(String taskId,
                                      String failedNodeId,
                                      String tableId,
                                      String errorCode,
                                      String errorMessage) {
        return new DqlStormGuardKey(taskId, failedNodeId, tableId, errorCode,
                normalizeMessage(errorMessage));
    }

    public static String normalizeMessage(String errorMessage) {
        if (errorMessage == null || errorMessage.trim().isEmpty()) {
            return UNKNOWN_DIMENSION;
        }
        String normalized = Normalizer.normalize(errorMessage, Normalizer.Form.NFKC)
                .trim()
                .toLowerCase(Locale.ROOT);
        normalized = UUID.matcher(normalized).replaceAll("<uuid>");
        normalized = IPV4.matcher(normalized).replaceAll("<ip>");
        normalized = HEX.matcher(normalized).replaceAll("<hex>");
        normalized = NUMBER.matcher(normalized).replaceAll("<number>");
        normalized = WHITESPACE.matcher(normalized).replaceAll(" ");
        if (normalized.length() > MAX_NORMALIZED_MESSAGE_LENGTH) {
            normalized = normalized.substring(0, MAX_NORMALIZED_MESSAGE_LENGTH);
        }
        return normalized;
    }

    private static String normalizeDimension(String dimension) {
        if (dimension == null || dimension.trim().isEmpty()) {
            return UNKNOWN_DIMENSION;
        }
        return dimension.trim();
    }

    public String getTaskId() {
        return taskId;
    }

    public String getFailedNodeId() {
        return failedNodeId;
    }

    public String getTableId() {
        return tableId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getNormalizedErrorMessage() {
        return normalizedErrorMessage;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DqlStormGuardKey)) {
            return false;
        }
        DqlStormGuardKey that = (DqlStormGuardKey) other;
        return taskId.equals(that.taskId)
                && failedNodeId.equals(that.failedNodeId)
                && tableId.equals(that.tableId)
                && errorCode.equals(that.errorCode)
                && normalizedErrorMessage.equals(that.normalizedErrorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, failedNodeId, tableId, errorCode, normalizedErrorMessage);
    }

    @Override
    public String toString() {
        return "taskId=" + taskId
                + ", failedNodeId=" + failedNodeId
                + ", tableId=" + tableId
                + ", errorCode=" + errorCode
                + ", normalizedErrorMessage=" + normalizedErrorMessage;
    }
}

package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventListVo;
import com.tapdata.tm.dql.vo.DqlRecoveryAttemptVo;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Maps persistence DTOs to the deliberately smaller, safe Web query representations. */
final class DqlEventWebMapper {
    private static final int PREVIEW_FIELD_MAX_LENGTH = 512;
    private static final int PREVIEW_MAX_DEPTH = 4;
    private static final int PREVIEW_MAX_ITEMS = 50;
    private static final String MASKED_VALUE = "******";
    private static final String TRUNCATED_MARKER = "...";
    private static final Set<String> SENSITIVE_FIELDS = Set.of(
            "password", "passwd", "secret", "token", "access_token", "authorization", "credential", "apikey"
    );

    private final ObjectMapper canonicalObjectMapper = new ObjectMapper()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    DqlEventListVo toList(DqlEventDto event) {
        if (event == null) {
            return null;
        }
        DqlEventListVo result = new DqlEventListVo();
        result.setId(event.getId());
        result.setEventId(event.getEventId());
        result.setTaskId(event.getTaskId());
        result.setTaskName(event.getTaskName());
        result.setSourceTable(event.getSourceTable());
        result.setTargetTable(event.getTargetTable());
        result.setDmlType(event.getDmlType());
        result.setErrorType(event.getErrorType());
        result.setErrorCode(event.getErrorCode());
        result.setEventTime(event.getEventTime());
        result.setFailedAt(event.getFailedAt());
        result.setCaptureSeq(event.getCaptureSeq());
        result.setStatus(event.getStatus());
        result.setRecoveryCount(event.getRecoveryCount());
        result.setLastRecoveryTime(event.getLastRecoveryTime());
        return result;
    }

    DqlEventDetailVo toDetail(DqlEventDto event) {
        if (event == null) {
            return null;
        }
        DqlEventDetailVo result = new DqlEventDetailVo();
        result.setId(event.getId());
        result.setEventId(event.getEventId());
        result.setTaskId(event.getTaskId());
        result.setTaskName(event.getTaskName());
        result.setSourceTable(event.getSourceTable());
        result.setTargetTable(event.getTargetTable());
        result.setDmlType(event.getDmlType());
        result.setErrorType(event.getErrorType());
        result.setErrorCode(event.getErrorCode());
        result.setEventTime(event.getEventTime());
        result.setFailedAt(event.getFailedAt());
        result.setCaptureSeq(event.getCaptureSeq());
        result.setRecoveryCount(event.getRecoveryCount());
        result.setLastRecoveryTime(event.getLastRecoveryTime());
        result.setSourceNodeName(event.getSourceNodeName());
        result.setTargetNodeName(event.getTargetNodeName());
        result.setFailedNodeName(event.getFailedNodeName());
        result.setStage(event.getFailedStage());
        result.setTableId(event.getTableId());
        result.setEventKey(toEventKey(event.getEventKey()));
        result.setEventKeyMissing(event.getEventKeyMissing());
        result.setPayloadFormat(event.getPayloadFormat());
        result.setPayloadHash(event.getPayloadHash());
        result.setPayloadSize(event.getPayloadSize());
        result.setPayloadComplete(event.getPayloadComplete());
        SanitizedPreview preview = sanitizePreview(event.getPayloadPreview());
        result.setPayloadPreview(preview.value());
        result.setPayloadPreviewTruncated(Boolean.TRUE.equals(event.getPayloadPreviewTruncated()) || preview.truncated());
        result.setErrorDetails(event.getErrorDetails());
        result.setRawErrorRef(event.getRawErrorRef());
        result.setRecoveryAttempts(toAttempts(event.getRecoveryAttempts()));
        return result;
    }

    private List<DqlRecoveryAttemptVo> toAttempts(List<DqlRecoveryAttemptDto> attempts) {
        if (attempts == null || attempts.isEmpty()) {
            return attempts == null ? null : List.of();
        }
        int fromIndex = Math.max(0, attempts.size() - 20);
        List<DqlRecoveryAttemptVo> result = new ArrayList<>(attempts.size() - fromIndex);
        for (int index = attempts.size() - 1; index >= fromIndex; index--) {
            result.add(toAttempt(attempts.get(index)));
        }
        return result;
    }

    private DqlRecoveryAttemptVo toAttempt(DqlRecoveryAttemptDto attempt) {
        DqlRecoveryAttemptVo result = new DqlRecoveryAttemptVo();
        result.setAttemptId(attempt.getAttemptId());
        result.setBatchId(attempt.getBatchId());
        result.setStartedAt(attempt.getStartedAt());
        result.setFinishedAt(attempt.getFinishedAt());
        result.setResult(attempt.getResult());
        result.setMessage(attempt.getMessage());
        result.setErrorCode(attempt.getErrorCode());
        result.setErrorMessage(attempt.getErrorDetails());
        return result;
    }

    private String toEventKey(Map<String, Object> eventKey) {
        if (eventKey == null || eventKey.isEmpty()) {
            return null;
        }
        try {
            return canonicalObjectMapper.writeValueAsString(sanitizePreview(eventKey).value());
        } catch (JsonProcessingException e) {
            throw new BizException("DqlEvent.InvalidPayload", e.getOriginalMessage());
        }
    }

    private SanitizedPreview sanitizePreview(Map<?, ?> preview) {
        if (preview == null) {
            return new SanitizedPreview(null, false);
        }
        PreviewContext context = new PreviewContext();
        Map<String, Object> value = sanitizeMap(preview, 0, context);
        return new SanitizedPreview(value, context.truncated);
    }

    private Map<String, Object> sanitizeMap(Map<?, ?> source, int depth, PreviewContext context) {
        Map<String, Object> result = new LinkedHashMap<>();
        int count = 0;
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (count >= PREVIEW_MAX_ITEMS) {
                context.truncated = true;
                break;
            }
            String field = String.valueOf(entry.getKey());
            result.put(field, isSensitive(field)
                    ? MASKED_VALUE
                    : sanitizeValue(entry.getValue(), depth + 1, context));
            count++;
        }
        return result;
    }

    private Object sanitizeValue(Object value, int depth, PreviewContext context) {
        if (value == null || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof CharSequence sequence) {
            return truncateString(sequence.toString(), context);
        }
        if (value instanceof Map<?, ?> map) {
            if (depth > PREVIEW_MAX_DEPTH) {
                context.truncated = true;
                return TRUNCATED_MARKER;
            }
            return sanitizeMap(map, depth, context);
        }
        if (value instanceof Collection<?> collection) {
            if (depth > PREVIEW_MAX_DEPTH) {
                context.truncated = true;
                return TRUNCATED_MARKER;
            }
            return sanitizeCollection(collection, depth, context);
        }
        if (value.getClass().isArray()) {
            if (depth > PREVIEW_MAX_DEPTH) {
                context.truncated = true;
                return TRUNCATED_MARKER;
            }
            return sanitizeArray(value, depth, context);
        }
        return truncateString(String.valueOf(value), context);
    }

    private List<Object> sanitizeCollection(Collection<?> source, int depth, PreviewContext context) {
        List<Object> result = new ArrayList<>(Math.min(source.size(), PREVIEW_MAX_ITEMS));
        int count = 0;
        for (Object value : source) {
            if (count >= PREVIEW_MAX_ITEMS) {
                context.truncated = true;
                break;
            }
            result.add(sanitizeValue(value, depth + 1, context));
            count++;
        }
        return result;
    }

    private List<Object> sanitizeArray(Object source, int depth, PreviewContext context) {
        int length = Array.getLength(source);
        int kept = Math.min(length, PREVIEW_MAX_ITEMS);
        List<Object> result = new ArrayList<>(kept);
        for (int index = 0; index < kept; index++) {
            result.add(sanitizeValue(Array.get(source, index), depth + 1, context));
        }
        if (length > PREVIEW_MAX_ITEMS) {
            context.truncated = true;
        }
        return result;
    }

    private String truncateString(String value, PreviewContext context) {
        if (value.length() <= PREVIEW_FIELD_MAX_LENGTH) {
            return value;
        }
        context.truncated = true;
        return value.substring(0, PREVIEW_FIELD_MAX_LENGTH);
    }

    private boolean isSensitive(String field) {
        String normalized = field.replace("_", "").replace("-", "").toLowerCase(Locale.ROOT);
        return SENSITIVE_FIELDS.stream()
                .map(value -> value.replace("_", "").replace("-", ""))
                .anyMatch(normalized::equals);
    }

    private record SanitizedPreview(Map<String, Object> value, boolean truncated) {
    }

    private static final class PreviewContext {
        private boolean truncated;
    }
}

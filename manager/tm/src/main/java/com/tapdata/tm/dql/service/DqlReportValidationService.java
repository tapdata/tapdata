package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.DqlExceptionScopeEnum;
import com.tapdata.tm.dql.DqlRouteDecisionEnum;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.task.repository.TaskRepository;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Applies the TM-side trust boundary for Engine DQL reports before identity generation and persistence.
 */
@Service
public class DqlReportValidationService {
    static final int ERROR_DETAILS_MAX_LENGTH = 4000;
    static final long PAYLOAD_MAX_BYTES = 1_048_576L;
    static final int PREVIEW_FIELD_MAX_LENGTH = 512;
    static final int PREVIEW_MAX_DEPTH = 4;
    static final int PREVIEW_MAX_ITEMS = 50;
    static final String PREVIEW_TRUNCATED_MARKER = "...";
    static final String MASKED_VALUE = "******";

    private static final Set<String> SENSITIVE_FIELDS = Set.of(
            "password",
            "passwd",
            "secret",
            "token",
            "access_token",
            "authorization",
            "credential",
            "apikey"
    );
    private static final Pattern SENSITIVE_ERROR_FIELD = Pattern.compile(
            "(?i)(?<![A-Za-z0-9_])([\\\"']?)(password|passwd|secret|token|access_token|authorization|credential|apikey)\\1\\s*[:=]\\s*"
    );

    private final TaskRepository taskRepository;
    private final ObjectMapper objectMapper;

    DqlReportValidationService() {
        this(null, new ObjectMapper());
    }

    @Autowired
    public DqlReportValidationService(TaskRepository taskRepository, ObjectMapper objectMapper) {
        this.taskRepository = taskRepository;
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    public ValidationResult validateAndSecure(String taskId, DqlEventReportVo report) {
        validateTask(taskId);
        validateRouteMetadata(report);
        boolean errorDetailsTruncated = secureErrorDetails(report);
        securePayload(report);
        securePreview(report);
        return new ValidationResult(errorDetailsTruncated);
    }

    private void validateTask(String taskId) {
        if (StringUtils.isBlank(taskId) || !ObjectId.isValid(taskId)) {
            throw new BizException("IllegalArgument", "taskId");
        }
        ObjectId objectId = new ObjectId(taskId);
        if (taskRepository != null && !taskRepository.existsById(objectId)) {
            throw new BizException("Task.NotFound", taskId);
        }
    }

    private void validateRouteMetadata(DqlEventReportVo report) {
        if (StringUtils.isBlank(report.getExceptionScope())) {
            report.setExceptionScope(DqlExceptionScopeEnum.RECORD.name());
        } else if (DqlExceptionScopeEnum.RECORD != DqlExceptionScopeEnum.parse(report.getExceptionScope())) {
            throw new BizException("DqlEvent.InvalidRouteDecision", "exceptionScope");
        } else {
            report.setExceptionScope(DqlExceptionScopeEnum.RECORD.name());
        }
        if (StringUtils.isBlank(report.getRouteDecision())) {
            report.setRouteDecision(DqlRouteDecisionEnum.RECORD_DLQ.name());
        } else if (DqlRouteDecisionEnum.RECORD_DLQ != DqlRouteDecisionEnum.parse(report.getRouteDecision())) {
            throw new BizException("DqlEvent.InvalidRouteDecision", "routeDecision");
        } else {
            report.setRouteDecision(DqlRouteDecisionEnum.RECORD_DLQ.name());
        }
    }

    private boolean secureErrorDetails(DqlEventReportVo report) {
        String errorDetails = report.getErrorDetails();
        if (errorDetails == null) {
            return false;
        }
        String secured = maskSensitiveErrorDetails(errorDetails);
        boolean truncated = secured.length() > ERROR_DETAILS_MAX_LENGTH;
        report.setErrorDetails(truncated ? secured.substring(0, ERROR_DETAILS_MAX_LENGTH) : secured);
        return truncated;
    }

    private String maskSensitiveErrorDetails(String errorDetails) {
        Matcher matcher = SENSITIVE_ERROR_FIELD.matcher(errorDetails);
        StringBuilder result = new StringBuilder(errorDetails.length());
        int cursor = 0;
        while (matcher.find(cursor)) {
            result.append(errorDetails, cursor, matcher.end());
            int valueStart = matcher.end();
            if (valueStart >= errorDetails.length()) {
                result.append(MASKED_VALUE);
                cursor = valueStart;
                break;
            }
            char first = errorDetails.charAt(valueStart);
            if (first == '\"' || first == '\'') {
                int valueEnd = findClosingQuote(errorDetails, valueStart + 1, first);
                result.append(first).append(MASKED_VALUE);
                if (valueEnd >= 0) {
                    result.append(first);
                    cursor = valueEnd + 1;
                } else {
                    cursor = errorDetails.length();
                }
            } else if (first == '{' || first == '[') {
                result.append(MASKED_VALUE);
                int valueEnd = findClosingStructure(errorDetails, valueStart);
                cursor = valueEnd >= 0 ? valueEnd + 1 : errorDetails.length();
            } else {
                result.append(MASKED_VALUE);
                cursor = findErrorLineEnd(errorDetails, valueStart);
            }
        }
        result.append(errorDetails, cursor, errorDetails.length());
        return result.toString();
    }

    private int findClosingQuote(String value, int start, char quote) {
        boolean escaped = false;
        for (int index = start; index < value.length(); index++) {
            char current = value.charAt(index);
            if (escaped) {
                escaped = false;
            } else if (current == '\\') {
                escaped = true;
            } else if (current == quote) {
                return index;
            }
        }
        return -1;
    }

    private int findClosingStructure(String value, int start) {
        Deque<Character> expectedClosings = new ArrayDeque<>();
        char quote = 0;
        boolean escaped = false;
        for (int index = start; index < value.length(); index++) {
            char current = value.charAt(index);
            if (quote != 0) {
                if (escaped) {
                    escaped = false;
                } else if (current == '\\') {
                    escaped = true;
                } else if (current == quote) {
                    quote = 0;
                }
                continue;
            }
            if (current == '\"' || current == '\'') {
                quote = current;
            } else if (current == '{') {
                expectedClosings.push('}');
            } else if (current == '[') {
                expectedClosings.push(']');
            } else if (current == '}' || current == ']') {
                if (expectedClosings.isEmpty() || expectedClosings.pop() != current) {
                    return -1;
                }
                if (expectedClosings.isEmpty()) {
                    return index;
                }
            }
        }
        return -1;
    }

    private int findErrorLineEnd(String value, int start) {
        for (int index = start; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '\r' || current == '\n') {
                return index;
            }
        }
        return value.length();
    }

    private void securePayload(DqlEventReportVo report) {
        Object payloadData = report.getPayloadData();
        long actualSize = serializedSize(payloadData);
        long declaredSize = report.getPayloadSize() == null ? 0L : Math.max(0L, report.getPayloadSize());
        long effectiveSize = Math.max(actualSize, declaredSize);
        report.setPayloadSize(effectiveSize);

        boolean complete = payloadData != null && !Boolean.FALSE.equals(report.getPayloadComplete());
        if (effectiveSize > PAYLOAD_MAX_BYTES) {
            report.setPayloadData(null);
            complete = false;
        }
        report.setPayloadComplete(complete);
    }

    private long serializedSize(Object payloadData) {
        if (payloadData == null) {
            return 0L;
        }
        try {
            return objectMapper.writeValueAsBytes(payloadData).length;
        } catch (JsonProcessingException e) {
            throw new BizException("DqlEvent.InvalidPayload", e.getOriginalMessage());
        }
    }

    private void securePreview(DqlEventReportVo report) {
        Map<String, Object> preview = report.getPayloadPreview();
        if (preview == null) {
            report.setPayloadPreviewTruncated(Boolean.TRUE.equals(report.getPayloadPreviewTruncated()));
            return;
        }
        PreviewContext context = new PreviewContext();
        report.setPayloadPreview(sanitizeMap(preview, 0, context));
        report.setPayloadPreviewTruncated(Boolean.TRUE.equals(report.getPayloadPreviewTruncated()) || context.truncated);
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
            if (isSensitive(field)) {
                result.put(field, MASKED_VALUE);
            } else {
                result.put(field, sanitizeValue(entry.getValue(), depth + 1, context));
            }
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
                return PREVIEW_TRUNCATED_MARKER;
            }
            return sanitizeMap(map, depth, context);
        }
        if (value instanceof Collection<?> collection) {
            if (depth > PREVIEW_MAX_DEPTH) {
                context.truncated = true;
                return PREVIEW_TRUNCATED_MARKER;
            }
            return sanitizeCollection(collection, depth, context);
        }
        if (value.getClass().isArray()) {
            if (depth > PREVIEW_MAX_DEPTH) {
                context.truncated = true;
                return PREVIEW_TRUNCATED_MARKER;
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
        return SENSITIVE_FIELDS.contains(field.toLowerCase(Locale.ROOT));
    }

    public record ValidationResult(boolean errorDetailsTruncated) {
    }

    private static class PreviewContext {
        private boolean truncated;
    }
}

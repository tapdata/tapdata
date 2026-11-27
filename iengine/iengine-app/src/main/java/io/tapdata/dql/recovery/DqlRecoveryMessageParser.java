package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Converts the legacy pipe Map into the typed DQL recovery command. */
public final class DqlRecoveryMessageParser {
    public static final int MAX_EVENT_COUNT = 200;

    private DqlRecoveryMessageParser() {
    }

    public static DqlRecoveryMessageDto parse(Map<?, ?> payload) {
        return parse(payload, MAX_EVENT_COUNT);
    }

    public static DqlRecoveryMessageDto parse(Map<?, ?> payload, int maxEventCount) {
        if (maxEventCount <= 0) {
            throw new DqlRecoveryMessageValidationException("maximum recovery batch size must be greater than zero");
        }
        if (payload == null) {
            throw new DqlRecoveryMessageValidationException("recovery message payload must not be null");
        }
        String type = text(payload.get("type"));
        if (!DqlRecoveryMessageDto.TYPE.equals(type)) {
            throw new DqlRecoveryMessageValidationException("recovery message type must be dqlRecovery");
        }

        DqlRecoveryMessageDto command = new DqlRecoveryMessageDto();
        command.setType(type);
        command.setTaskId(requiredText(payload, "taskId"));
        command.setBatchId(requiredText(payload, "batchId"));
        command.setTaskVersion(requiredVersion(payload.get("taskVersion")));
        command.setOrderedEventIds(requiredEventIds(payload.get("orderedEventIds"), maxEventCount));
        command.setOperatorId(optionalText(payload.get("operatorId")));
        command.setOperatorName(optionalText(payload.get("operatorName")));
        String mode = optionalText(payload.get("mode"));
        command.setMode(StringUtils.defaultIfBlank(mode, DqlRecoveryMessageDto.MODE_AUTO));
        if (!DqlRecoveryMessageDto.MODE_AUTO.equals(command.getMode())) {
            throw new DqlRecoveryMessageValidationException("recovery message mode must be AUTO");
        }
        return command;
    }

    private static String requiredText(Map<?, ?> payload, String field) {
        String value = optionalText(payload.get(field));
        if (StringUtils.isBlank(value)) {
            throw new DqlRecoveryMessageValidationException(field + " must not be blank");
        }
        return value;
    }

    private static String optionalText(Object value) {
        return value == null ? null : text(value);
    }

    private static String text(Object value) {
        return value == null ? null : String.valueOf(value).trim();
    }

    private static Long requiredVersion(Object value) {
        if (value == null) {
            throw new DqlRecoveryMessageValidationException("taskVersion must not be null");
        }
        if (value instanceof Number number) {
            long version = number.longValue();
            if (number.doubleValue() != version) {
                throw new DqlRecoveryMessageValidationException("taskVersion must be an integer");
            }
            if (version < 0) {
                throw new DqlRecoveryMessageValidationException("taskVersion must not be negative");
            }
            return version;
        }
        try {
            long version = Long.parseLong(String.valueOf(value).trim());
            if (version < 0) {
                throw new DqlRecoveryMessageValidationException("taskVersion must not be negative");
            }
            return version;
        } catch (NumberFormatException exception) {
            throw new DqlRecoveryMessageValidationException("taskVersion must be an integer");
        }
    }

    private static List<String> requiredEventIds(Object value, int maxEventCount) {
        if (!(value instanceof Collection<?> values) || values.isEmpty()) {
            throw new DqlRecoveryMessageValidationException("orderedEventIds must not be empty");
        }
        if (values.size() > maxEventCount) {
            throw new DqlRecoveryMessageValidationException("orderedEventIds exceeds the maximum batch size");
        }
        List<String> eventIds = new ArrayList<>(values.size());
        Set<String> uniqueIds = new HashSet<>();
        for (Object item : values) {
            String eventId = optionalText(item);
            if (StringUtils.isBlank(eventId)) {
                throw new DqlRecoveryMessageValidationException("orderedEventIds must contain no blank event id");
            }
            if (!uniqueIds.add(eventId)) {
                throw new DqlRecoveryMessageValidationException("orderedEventIds must not contain duplicates");
            }
            eventIds.add(eventId);
        }
        return List.copyOf(eventIds);
    }
}

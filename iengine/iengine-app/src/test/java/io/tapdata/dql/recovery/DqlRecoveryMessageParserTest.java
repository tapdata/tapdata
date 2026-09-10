package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryMessageParserTest {
    @Test
    @DisplayName("parses pipe values into the typed command and defaults mode to AUTO")
    void parsesTypedCommand() {
        DqlRecoveryMessageDto command = DqlRecoveryMessageParser.parse(Map.of(
                "type", "dqlRecovery",
                "taskId", "task-1",
                "batchId", "batch-1",
                "taskVersion", "7",
                "orderedEventIds", List.of(" event-2 ", "event-1")
        ));

        assertEquals("dqlRecovery", command.getType());
        assertEquals(7L, command.getTaskVersion());
        assertEquals(List.of("event-2", "event-1"), command.getOrderedEventIds());
        assertEquals(DqlRecoveryMessageDto.MODE_AUTO, command.getMode());
        assertThrows(UnsupportedOperationException.class,
                () -> command.getOrderedEventIds().add("event-3"));
    }

    @Test
    @DisplayName("rejects invalid type, version, mode and event identity")
    void rejectsInvalidCommand() {
        assertRejected(Map.of("type", "other"), "type");
        assertRejected(messageWithout("taskVersion"), "taskVersion");
        assertRejected(messageWith("taskVersion", 1.5), "integer");
        assertRejected(messageWith("mode", "MANUAL"), "mode");
        assertRejected(messageWith("orderedEventIds", List.of("event-1", "event-1")), "duplicates");
    }

    private void assertRejected(Map<String, Object> payload, String message) {
        DqlRecoveryMessageValidationException exception = assertThrows(
                DqlRecoveryMessageValidationException.class,
                () -> DqlRecoveryMessageParser.parse(payload)
        );
        assertTrue(exception.getMessage().contains(message));
    }

    private Map<String, Object> messageWithout(String field) {
        java.util.LinkedHashMap<String, Object> message = new java.util.LinkedHashMap<>(baseMessage());
        message.remove(field);
        return message;
    }

    private Map<String, Object> messageWith(String field, Object value) {
        java.util.LinkedHashMap<String, Object> message = new java.util.LinkedHashMap<>(baseMessage());
        message.put(field, value);
        return message;
    }

    private Map<String, Object> baseMessage() {
        return Map.of(
                "type", "dqlRecovery",
                "taskId", "task-1",
                "batchId", "batch-1",
                "taskVersion", 7L,
                "orderedEventIds", List.of("event-2", "event-1")
        );
    }
}

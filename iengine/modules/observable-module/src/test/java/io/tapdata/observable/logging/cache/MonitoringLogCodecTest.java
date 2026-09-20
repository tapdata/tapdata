package io.tapdata.observable.logging.cache;

import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MonitoringLogCodecTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRoundTripHistoricalWireFieldsInOrder() {
        MonitoringLogCodec codec = new MonitoringLogCodec();
        Map<String, Object> data = new HashMap<>();
        data.put("id", "record-id");
        MonitoringLogsDto source = MonitoringLogsDto.builder()
                .date(new Date(1_700_000_000_123L))
                .level("ERROR")
                .errorStack("stack")
                .message("message")
                .taskId("task-id")
                .taskRecordId("record-id")
                .timestamp(1_700_000_000_123L)
                .taskName("task-name")
                .nodeId("node-id")
                .nodeName("node-name")
                .errorCode("11001")
                .fullErrorCode("TAP11001")
                .dynamicDescriptionParameters(new String[]{"a", "b"})
                .logTags(Collections.singletonList("tag"))
                .data(Collections.singletonList(data))
                .build();

        AtomicReference<MonitoringLogsDto> result = new AtomicReference<>();
        try (ChronicleQueue queue = ChronicleQueue.singleBuilder(tempDir).build()) {
            try (ExcerptAppender appender = queue.acquireAppender()) {
                appender.writeDocument(wire -> codec.write(wire.getValueOut(), source));
            }
            try (ExcerptTailer tailer = queue.createTailer()) {
                assertTrue(tailer.readDocument(wire -> result.set(codec.read(wire.getValueIn()))));
            }
        }

        MonitoringLogsDto restored = result.get();
        assertNotNull(restored);
        assertEquals(source.getDate(), restored.getDate());
        assertEquals(source.getLevel(), restored.getLevel());
        assertEquals(source.getErrorStack(), restored.getErrorStack());
        assertEquals(source.getMessage(), restored.getMessage());
        assertEquals(source.getTaskId(), restored.getTaskId());
        assertEquals(source.getTaskRecordId(), restored.getTaskRecordId());
        assertEquals(source.getTimestamp(), restored.getTimestamp());
        assertEquals(source.getTaskName(), restored.getTaskName());
        assertEquals(source.getNodeId(), restored.getNodeId());
        assertEquals(source.getNodeName(), restored.getNodeName());
        assertEquals(source.getErrorCode(), restored.getErrorCode());
        assertEquals(source.getFullErrorCode(), restored.getFullErrorCode());
        assertArrayEquals(source.getDynamicDescriptionParameters(), restored.getDynamicDescriptionParameters());
        assertEquals(source.getLogTags(), restored.getLogTags());
        assertEquals("record-id", restored.getData().get(0).get("id"));
    }
}

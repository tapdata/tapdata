package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AiStreamEventWriterTest {

    @Test
    void writesNamedJsonSseEvent() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AiStreamEventWriter writer = new AiStreamEventWriter(new ObjectMapper(), outputStream);

        writer.send("message_delta", Map.of("content", "hello"));

        assertEquals("event: message_delta\ndata: {\"content\":\"hello\"}\n\n",
                outputStream.toString(StandardCharsets.UTF_8));
    }

    @Test
    void flushesServletResponseBufferAfterEachEvent() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AtomicInteger flushBufferCalls = new AtomicInteger();
        AiStreamEventWriter writer = new AiStreamEventWriter(new ObjectMapper(), outputStream,
                flushBufferCalls::incrementAndGet);

        writer.send("message_delta", Map.of("content", "hello"));
        writer.send("done", Map.of("finishReason", "stop"));

        assertEquals(2, flushBufferCalls.get());
    }

    @Test
    void writesTypedErrorEvent() throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AiStreamEventWriter writer = new AiStreamEventWriter(new ObjectMapper(), outputStream);

        writer.error("llm provider rejected the request");

        String payload = outputStream.toString(StandardCharsets.UTF_8);
        assertTrue(payload.startsWith("event: error\n"));
        assertTrue(payload.contains("\"message\":\"llm provider rejected the request\""));
    }
}

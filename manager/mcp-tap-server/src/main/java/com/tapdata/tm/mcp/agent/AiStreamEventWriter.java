package com.tapdata.tm.mcp.agent;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class AiStreamEventWriter {

    private final ObjectMapper objectMapper;
    private final OutputStream outputStream;
    private final ResponseBufferFlusher responseBufferFlusher;

    public AiStreamEventWriter(ObjectMapper objectMapper, OutputStream outputStream) {
        this(objectMapper, outputStream, null);
    }

    public AiStreamEventWriter(ObjectMapper objectMapper, OutputStream outputStream,
                               ResponseBufferFlusher responseBufferFlusher) {
        this.objectMapper = objectMapper;
        this.outputStream = outputStream;
        this.responseBufferFlusher = responseBufferFlusher;
    }

    public synchronized void send(String event, Object data) throws IOException {
        writeLine("event: " + event);
        String json = objectMapper.writeValueAsString(data);
        for (String line : json.split("\\R", -1)) {
            writeLine("data: " + line);
        }
        writeLine("");
        outputStream.flush();
        if (responseBufferFlusher != null) {
            responseBufferFlusher.flushBuffer();
        }
    }

    public void messageDelta(String content) throws IOException {
        send("message_delta", Map.of("content", content));
    }

    public void toolCallStart(String id, String name, Map<String, Object> arguments) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", id);
        payload.put("name", name);
        payload.put("arguments", arguments);
        send("tool_call_start", payload);
    }

    public void toolCallResult(String id, String name, Object result) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", id);
        payload.put("name", name);
        payload.put("result", result);
        send("tool_call_result", payload);
    }

    public void done() throws IOException {
        send("done", Map.of("finishReason", "stop"));
    }

    public void error(String message) throws IOException {
        send("error", Map.of("message", message == null ? "Unknown error" : message));
    }

    private void writeLine(String line) throws IOException {
        outputStream.write(line.getBytes(StandardCharsets.UTF_8));
        outputStream.write('\n');
    }

    @FunctionalInterface
    public interface ResponseBufferFlusher {
        void flushBuffer() throws IOException;
    }
}

package com.tapdata.tm.trace.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.trace.dto.TraceStreamEvent;
import com.tapdata.tm.trace.param.WideTableTraceRequest;
import com.tapdata.tm.trace.service.TraceService;
import com.tapdata.tm.trace.service.date.TraceDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
public class TraceServiceImpl implements TraceService {

    private final ObjectMapper objectMapper;
    @Autowired
    private TraceDataService traceDataService;

    public TraceServiceImpl(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void streamWideTableTrace(WideTableTraceRequest request, OutputStream outputStream) throws IOException {
        String requestId = "trace_" + UUID.randomUUID().toString().replace("-", "");
        traceDataService.traceData(request, requestId, outputStream);
        writeEvent(outputStream, TraceStreamEvent.complete(requestId));
    }

    private void writeEvent(OutputStream outputStream, TraceStreamEvent event) throws IOException {
        outputStream.write(objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8));
        outputStream.write('\n');
        outputStream.flush();
    }
}

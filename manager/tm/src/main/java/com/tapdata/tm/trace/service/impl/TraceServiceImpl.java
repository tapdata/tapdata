package com.tapdata.tm.trace.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.trace.dto.TraceNodeError;
import com.tapdata.tm.trace.dto.TraceStreamEvent;
import com.tapdata.tm.trace.param.WideTableTraceRequest;
import com.tapdata.tm.trace.service.TraceService;
import com.tapdata.tm.trace.service.data.TraceDataService;
import com.tapdata.tm.utils.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
@Slf4j
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
        try {
            validateFilters(request);
            traceDataService.traceData(request, requestId, outputStream);
        } catch (BizException e) {
            writeEvent(outputStream, TraceStreamEvent.error(requestId, toError(e.getErrorCode(), e.getMessage())));
        } catch (Exception e) {
            log.error("Wide table trace failed, requestId: {}", requestId, e);
            writeEvent(outputStream, TraceStreamEvent.error(requestId,
                    toError(BizException.SYSTEM_ERROR, MessageUtil.getMessage(BizException.SYSTEM_ERROR, e.getMessage()))));
        }
        writeEvent(outputStream, TraceStreamEvent.complete(requestId));
    }

    private void validateFilters(WideTableTraceRequest request) {
        if (request.getFilters() == null
                || ((CollectionUtils.isEmpty(request.getFilters().getCustom()) || request.getFilters().getCustom().contains(null))
                && StringUtils.isBlank(request.getFilters().getSql()))) {
            throw new BizException("Trace.Filters.NotNull");
        }
    }

    private TraceNodeError toError(String code, String message) {
        TraceNodeError error = new TraceNodeError();
        error.setCode(code);
        error.setMessage(message);
        return error;
    }

    private void writeEvent(OutputStream outputStream, TraceStreamEvent event) throws IOException {
        outputStream.write(objectMapper.writeValueAsString(event).getBytes(StandardCharsets.UTF_8));
        outputStream.write('\n');
        outputStream.flush();
    }
}

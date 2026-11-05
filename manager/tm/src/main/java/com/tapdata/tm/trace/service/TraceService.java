package com.tapdata.tm.trace.service;

import com.tapdata.tm.trace.param.WideTableTraceRequest;

import java.io.IOException;
import java.io.OutputStream;

public interface TraceService {

    void streamWideTableTrace(WideTableTraceRequest request, OutputStream outputStream) throws IOException;
}

package com.tapdata.tm.monitor.service;

import com.tapdata.tm.monitor.dto.BatchRequestDto;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public interface BatchService {
    Map<String, Object> batch(BatchRequestDto batchRequestDto, Locale locale) throws ExecutionException, InterruptedException;
}

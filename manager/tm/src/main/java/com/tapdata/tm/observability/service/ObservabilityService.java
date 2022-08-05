package com.tapdata.tm.observability.service;

import com.tapdata.tm.observability.dto.BatchRequestDto;
import com.tapdata.tm.observability.vo.BatchResponeVo;

import java.util.concurrent.ExecutionException;

public interface ObservabilityService {
    BatchResponeVo batch(BatchRequestDto batchRequestDto) throws ExecutionException, InterruptedException;
}

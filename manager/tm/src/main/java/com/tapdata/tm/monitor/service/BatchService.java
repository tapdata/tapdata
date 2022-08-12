package com.tapdata.tm.monitor.service;

import com.tapdata.tm.monitor.dto.BatchRequestDto;
import com.tapdata.tm.monitor.vo.BatchResponeVo;

import java.util.concurrent.ExecutionException;

public interface BatchService {
    BatchResponeVo batch(BatchRequestDto batchRequestDto) throws ExecutionException, InterruptedException;
}

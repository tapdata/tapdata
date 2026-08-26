package com.tapdata.tm.dql.service;

import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import org.springframework.stereotype.Service;

@Service
public class DqlEventAlarmService {
    public void notifyEventCreated(DqlEventDto event) {
    }

    public void notifySaveFailed(String taskId, String reason) {
    }

    public void notifyRecoveryFailed(DqlRecoveryBatchDto batch) {
    }

    public void notifyBatchPartialFailed(DqlRecoveryBatchDto batch) {
    }
}

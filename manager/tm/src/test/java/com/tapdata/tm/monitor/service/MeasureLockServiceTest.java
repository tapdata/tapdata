package com.tapdata.tm.monitor.service;

import com.tapdata.tm.BaseJunit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.*;

class MeasureLockServiceTest extends BaseJunit {

    @Autowired
    MeasureLockService measureLockService;

    @Test
    void tryGetLock() {
        measureLockService.tryGetLock("234234");
    }
}
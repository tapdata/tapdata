package com.tapdata.tm.taskrebalance.constant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TaskRebalanceStatusTest {

    @Test
    @DisplayName("final status is OK when every job succeeds")
    void finalStatusOk() {
        assertEquals(TaskRebalanceStatus.OK, TaskRebalanceStatus.finalStatus(0, 0));
    }

    @Test
    @DisplayName("final status is CANCELLED when jobs are cancelled without failures")
    void finalStatusCancelled() {
        assertEquals(TaskRebalanceStatus.CANCELLED, TaskRebalanceStatus.finalStatus(0, 1));
    }

    @Test
    @DisplayName("final status stays FAILED when any job fails")
    void finalStatusFailed() {
        assertEquals(TaskRebalanceStatus.FAILED, TaskRebalanceStatus.finalStatus(1, 1));
    }
}

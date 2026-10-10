package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.dto.Field;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskControllerAlarmSummaryTest {

    @Test
    void summaryOnlyWhenRequestedExplicitly() {
        assertFalse(TaskController.requestsAlarmReceiverSummary(null));
        assertFalse(TaskController.requestsAlarmReceiverSummary(new Field()));

        Field excluded = new Field();
        excluded.put("alarmReceiverStatus", false);
        assertFalse(TaskController.requestsAlarmReceiverSummary(excluded));

        Field status = new Field();
        status.put("alarmReceiverStatus", true);
        assertTrue(TaskController.requestsAlarmReceiverSummary(status));

        Field count = new Field();
        count.put("effectiveEmailCount", 1);
        assertTrue(TaskController.requestsAlarmReceiverSummary(count));

        Field text = new Field();
        text.put("effectiveEmailCount", "true");
        assertTrue(TaskController.requestsAlarmReceiverSummary(text));
    }
}

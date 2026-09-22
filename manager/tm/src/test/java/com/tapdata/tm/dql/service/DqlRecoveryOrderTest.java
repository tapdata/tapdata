package com.tapdata.tm.dql.service;

import com.tapdata.tm.dql.dto.DqlEventDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DqlRecoveryOrderTest {

    @Test
    @DisplayName("sorts recovery events by task, event time, capture sequence and event id")
    void sortsByStableRecoveryOrder() {
        DqlEventDto taskB = event("task-b", "DQL-b", 100L, 1L);
        DqlEventDto laterEvent = event("task-a", "DQL-4", 200L, 1L);
        DqlEventDto laterCapture = event("task-a", "DQL-3", 100L, 2L);
        DqlEventDto sameCaptureSecondId = event("task-a", "DQL-2", 100L, 1L);
        DqlEventDto sameCaptureFirstId = event("task-a", "DQL-1", 100L, 1L);

        List<DqlEventDto> ordered = DqlRecoveryOrder.sort(List.of(
                taskB,
                laterEvent,
                laterCapture,
                sameCaptureSecondId,
                sameCaptureFirstId
        ));

        assertEquals(List.of("DQL-1", "DQL-2", "DQL-3", "DQL-4", "DQL-b"),
                ordered.stream().map(DqlEventDto::getEventId).toList());
    }

    private DqlEventDto event(String taskId, String eventId, long eventTime, long captureSeq) {
        DqlEventDto event = new DqlEventDto();
        event.setTaskId(taskId);
        event.setEventId(eventId);
        event.setEventTime(new Date(eventTime));
        event.setCaptureSeq(captureSeq);
        return event;
    }
}

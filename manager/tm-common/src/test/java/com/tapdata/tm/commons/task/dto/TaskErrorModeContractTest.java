package com.tapdata.tm.commons.task.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TaskErrorModeContractTest {

    @Test
    void recognizesDqlAsThePersistedTaskErrorMode() {
        TaskDto.SkipErrorEvent skipErrorEvent = new TaskDto.SkipErrorEvent();
        skipErrorEvent.setErrorMode("DQL");

        assertEquals("DQL", skipErrorEvent.getErrorModeEnum().name());
    }

    @Test
    void normalizesTheFormerDlqAliasToDql() {
        TaskDto.SkipErrorEvent skipErrorEvent = new TaskDto.SkipErrorEvent();
        skipErrorEvent.setErrorMode("DLQ");

        assertEquals("DQL", skipErrorEvent.getErrorMode());
        assertEquals("DQL", skipErrorEvent.getErrorModeEnum().name());
    }
}

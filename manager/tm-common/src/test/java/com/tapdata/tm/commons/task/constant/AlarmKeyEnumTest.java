package com.tapdata.tm.commons.task.constant;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AlarmKeyEnumTest {

    @Test
    void getTaskAlarmKeys() {
        Assertions.assertEquals(38, AlarmKeyEnum.getTaskAlarmKeys().size());
    }
}
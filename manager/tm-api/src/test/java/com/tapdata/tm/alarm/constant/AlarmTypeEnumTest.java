package com.tapdata.tm.alarm.constant;

import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AlarmTypeEnumTest {

    @Test
    void testGet() {
        assertEquals(6, AlarmTypeEnum.get(AlarmTypeEnum.TYPE_TASK).size());
        assertEquals(1, AlarmTypeEnum.get(AlarmTypeEnum.API_SERVER_ALARM.getType()).size());
    }

}
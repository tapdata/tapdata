package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompressWeekTest {
    @Test
    void testAfterPropertiesSet() {
        CompressWeek compressDay = new CompressWeek();
        compressDay.afterPropertiesSet();
        assertNotNull(Compress.Factory.get(Compress.Type.WEEK));
    }

    @Test
    void testCompressTime() {
        CompressWeek compressDay = new CompressWeek();
        WorkerCallEntity e = new WorkerCallEntity();
        e.setTimeStart(1756889869321L);
        long time = compressDay.compressTime(e);
        assertEquals(1756656000000L, time);
    }

    @Test
    void testPlus() {
        CompressWeek compressDay = new CompressWeek();
        long time = compressDay.plus(1756889869321L);
        assertEquals(1757260800000L, time);
    }
}
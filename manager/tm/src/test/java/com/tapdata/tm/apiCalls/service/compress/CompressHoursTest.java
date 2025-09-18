package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.service.compress.Compress;
import com.tapdata.tm.apiServer.service.compress.CompressHours;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompressHoursTest {
    @Test
    void testAfterPropertiesSet() {
        CompressHours compressDay = new CompressHours();
        compressDay.afterPropertiesSet();
        assertNotNull(Compress.Factory.get(Compress.Type.HOUR));
    }

    @Test
    void testCompressTime() {
        CompressHours compressDay = new CompressHours();
        WorkerCallEntity e = new WorkerCallEntity();
        e.setTimeStart(1L);
        long time = compressDay.compressTime(e);
        assertEquals(0L, time);
    }

    @Test
    void testPlus() {
        CompressHours compressDay = new CompressHours();
        long time = compressDay.plus(CompressHours.STEP);
        assertEquals(2 * CompressHours.STEP, time);
    }
}
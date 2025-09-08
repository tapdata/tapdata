package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompressDayTest {
    @Test
    void testAfterPropertiesSet() {
        CompressDay compressDay = new CompressDay();
        compressDay.afterPropertiesSet();
        assertNotNull(Compress.Factory.get(Compress.Type.DAY));
    }

    @Test
    void testCompressTime() {
        CompressDay compressDay = new CompressDay();
        WorkerCallEntity e = new WorkerCallEntity();
        e.setTimeStart(1L);
        long time = compressDay.compressTime(e);
        assertEquals(0L, time);
    }

    @Test
    void testPlus() {
        CompressDay compressDay = new CompressDay();
        long time = compressDay.plus(24 * 60 * 60 * 1000L);
        assertEquals(2 * 24 * 60 * 60 * 1000L, time);
    }
}
package com.tapdata.tm.apiServer.service.compress;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.service.compress.Compress;
import com.tapdata.tm.apiServer.service.compress.CompressMinute;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompressMinuteTest {
    @Test
    void testAfterPropertiesSet() {
        CompressMinute compressDay = new CompressMinute();
        compressDay.afterPropertiesSet();
        assertNotNull(Compress.Factory.get(Compress.Type.MINUTE));
    }

    @Test
    void testCompressTime() {
        CompressMinute compressDay = new CompressMinute();
        WorkerCallEntity e = new WorkerCallEntity();
        e.setTimeStart(1L);
        long time = compressDay.compressTime(e);
        assertEquals(0L, time);
    }

    @Test
    void testPlus() {
        CompressMinute compressDay = new CompressMinute();
        long time = compressDay.plus(CompressMinute.STEP);
        assertEquals(2 * CompressMinute.STEP, time);
    }
}
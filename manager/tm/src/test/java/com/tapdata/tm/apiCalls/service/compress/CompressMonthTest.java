package com.tapdata.tm.apiCalls.service.compress;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompressMonthTest {
    @Test
    void testAfterPropertiesSet() {
        CompressMonth compressDay = new CompressMonth();
        compressDay.afterPropertiesSet();
        assertNotNull(Compress.Factory.get(Compress.Type.MONTH));
    }

    @Test
    void testCompressTime() {
        CompressMonth compressDay = new CompressMonth();
        WorkerCallEntity e = new WorkerCallEntity();
        e.setTimeStart(1756889869321L);
        long time = compressDay.compressTime(e);
        assertEquals(1756684800000L, time);
    }

    @Test
    void testPlus() {
        CompressMonth compressDay = new CompressMonth();
        long time = compressDay.plus(1756889869321L);
        assertEquals(1759276800000L, time);
    }
}
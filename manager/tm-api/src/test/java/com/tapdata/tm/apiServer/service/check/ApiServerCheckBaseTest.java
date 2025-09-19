package com.tapdata.tm.apiServer.service.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ApiServerCheckBaseTest {
    class MockApiServerCheckBase implements ApiServerCheckBase {
        @Override
        public AlarmKeyEnum type() {
            return null;
        }
    }

    @Test
    void enable() {
        MockApiServerCheckBase base = new MockApiServerCheckBase();
        assertTrue(base.enable());
    }

    @Test
    void type() {
        MockApiServerCheckBase base = new MockApiServerCheckBase();
        assertNull(base.type());
    }

    @Test
    void sort() {
        MockApiServerCheckBase base = new MockApiServerCheckBase();
        assertEquals(0, base.sort());
    }

    @Test
    void testType() {
        MockApiServerCheckBase base = new MockApiServerCheckBase();
        assertNull(base.type());
    }
}
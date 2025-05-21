package com.tapdata.tm.taskinspect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/14 14:11 Create
 */
class TaskInspectConfigTest {

    @Test
    void testInit() {
        TaskInspectConfig config = new TaskInspectConfig().init(-1);
        Assertions.assertNotNull(config.getEnable());
        Assertions.assertNotNull(config.getMode());
        Assertions.assertNotNull(config.getIntelligent());
        Assertions.assertNotNull(config.getCustom());
    }

    @Test
    void testInitZero() {
        TaskInspectConfig config = new TaskInspectConfig().init(0);
        Assertions.assertNotNull(config.getCustom());
        Assertions.assertNull(config.getCustom().getCdc());
    }

    @Test
    void testInitMulti() {
        TaskInspectConfig config = new TaskInspectConfig().init(1);
        Assertions.assertNotNull(config.getCustom());
        Assertions.assertNotNull(config.getCustom().getCdc());
        Assertions.assertNull(config.getCustom().getCdc().getType());
    }
}

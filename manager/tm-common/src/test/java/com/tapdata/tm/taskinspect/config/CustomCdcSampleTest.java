package com.tapdata.tm.taskinspect.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/13 14:27 Create
 */
class CustomCdcSampleTest {
    @Test
    void testFill() {
        CustomCdcSample config1 = new CustomCdcSample().init(-1);
        CustomCdcSample config2 = new CustomCdcSample();
        Assertions.assertNull(config2.getCapacity());
        Assertions.assertNull(config2.getLimit());
        Assertions.assertNull(config2.getInterval());

        config2.fill(config1);
        Assertions.assertEquals(config1.getCapacity(), config2.getCapacity());
        Assertions.assertEquals(config1.getLimit(), config2.getLimit());
        Assertions.assertEquals(config1.getInterval(), config2.getInterval());
    }
}

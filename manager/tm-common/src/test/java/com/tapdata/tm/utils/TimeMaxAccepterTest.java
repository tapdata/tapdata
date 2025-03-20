package com.tapdata.tm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/25 18:48 Create
 */
class TimeMaxAccepterTest {

    @Test
    void testFirstTrueSecondFalse() {
        TimeMaxAccepter ins = new TimeMaxAccepter(1000, 1);
        Assertions.assertTrue(ins.check());
        Assertions.assertFalse(ins.check());
    }

}

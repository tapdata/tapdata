package com.tapdata.tm.system.api.enums;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OutputTypeTest {
    @Test
    void testNormal() {
        Assertions.assertEquals(OutputType.AUTO, OutputType.by(0));
        Assertions.assertEquals(OutputType.CUSTOM, OutputType.by(1));
        Assertions.assertEquals(OutputType.AUTO, OutputType.by(2));
    }
}
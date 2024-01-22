package com.tapdata.tm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AES256UtilTest {

    @Test
    void testParseByte2HexStr() {
        Assertions.assertEquals("636f6e74656e74",AES256Util.parseByte2HexStr("content".getBytes()));
    }
}

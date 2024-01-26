package com.tapdata.constant;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AES256UtilTest {

    @Test
    public void testParseByte2HexStr() {
        assertEquals("636f6e74656e74", AES256Util.parseByte2HexStr("content".getBytes()));
    }
}

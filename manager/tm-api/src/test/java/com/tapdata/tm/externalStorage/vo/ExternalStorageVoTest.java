package com.tapdata.tm.externalStorage.vo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExternalStorageVoTest {

    @Test
    void shouldMaskMongoUriPasswordOnSet() {
        ExternalStorageVo vo = new ExternalStorageVo();

        vo.setUri("mongodb://admin:password@localhost:27017/test");

        assertEquals("mongodb://admin:******@localhost:27017/test", vo.getUri());
    }

    @Test
    void shouldKeepNonMongoUriUntouched() {
        ExternalStorageVo vo = new ExternalStorageVo();

        vo.setUri("https://example.com/path");

        assertEquals("https://example.com/path", vo.getUri());
    }
}

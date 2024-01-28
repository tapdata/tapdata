package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpUtilsTest {

    @Test
    void testSendGetData() {
        final Map<String, String> headMap = new HashMap<>();
        final String result = HttpUtils.sendGetData("path", headMap);
        assertThat(result).isEqualTo("");
    }

    @Test
    void testSendPostData1() {
        assertThat(HttpUtils.sendPostData("path", "bodyJson", "userId")).isEqualTo("");
    }

    @Test
    void testSendPostData2() {
        assertThat(HttpUtils.sendPostData("path", "bodyJson")).isEqualTo("");
    }
}

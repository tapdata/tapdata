package com.tapdata.tm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/12/10 11:38
 */
public class WebUtilsTest {
    @Test
    void testUrlDecode() {

        String decodeStr = WebUtils.urlDecode("test%25fwq");

        Assertions.assertEquals("test%fwq", decodeStr);
    }
}

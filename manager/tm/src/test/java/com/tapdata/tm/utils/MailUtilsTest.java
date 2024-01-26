package com.tapdata.tm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MailUtilsTest {

    @Test
    void testReadHtmlToString() {
        Assertions.assertNotEquals(MailUtils.readHtmlToString("mailTemplate.html"),"");
    }
}

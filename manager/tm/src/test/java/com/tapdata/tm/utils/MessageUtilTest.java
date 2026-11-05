package com.tapdata.tm.utils;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageUtilTest {

    @Test
    void bundleLookupDoesNotFallbackToJvmDefaultLocale() {
        Locale original = Locale.getDefault();
        Locale.setDefault(Locale.CHINA);
        try {
            String message = MessageUtil.getBundleMessageOrNull(Locale.ENGLISH, "userLogsTemplate", "userLogs._default.create");

            assertEquals("User {user} created {moduleName} {parameter1}", message);
        } finally {
            Locale.setDefault(original);
        }
    }
}

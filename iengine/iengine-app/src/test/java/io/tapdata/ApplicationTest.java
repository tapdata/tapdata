package io.tapdata;

import com.tapdata.tm.utils.OEMReplaceUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;

public class ApplicationTest {
    @Nested
    class AddRollingFileAppenderTest {
        @Test
        void test1() {
            assertDoesNotThrow(() -> {
                Application.addRollingFileAppender("./workDir");
            });
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            assertEquals(2, context.getRootLogger().getAppenders().size());
        }

        @Test
        void test2() {
            try (MockedStatic<OEMReplaceUtil> oemReplaceUtilMockedStatic = mockStatic(OEMReplaceUtil.class)) {
                Map<String, Object> oemConfig = new HashMap<>();
                oemConfig.put("oem", "replace");
                oemReplaceUtilMockedStatic.when(() -> OEMReplaceUtil.getOEMConfigMap("log/replace.json")).thenReturn(oemConfig);
                Application.addRollingFileAppender("./workDir");
            }
        }
    }

    @Nested
    class getFileNameAfterOem {
        @Test
        void testNormal() {
            try (MockedStatic<OEMReplaceUtil> oem = mockStatic(OEMReplaceUtil.class)) {
                oem.when(() -> OEMReplaceUtil.replace("fileName", "log/replace.json")).thenReturn("app");
                String fileName = Application.getFileNameAfterOem("fileName");
                Assertions.assertEquals("app", fileName);
            }
        }

        @Test
        void testNull() {
            try (MockedStatic<OEMReplaceUtil> oem = mockStatic(OEMReplaceUtil.class)) {
                oem.when(() -> OEMReplaceUtil.replace("fileName", "log/replace.json")).thenReturn(null);
                String fileName = Application.getFileNameAfterOem("fileName");
                Assertions.assertEquals("fileName", fileName);
            }
        }
    }
}
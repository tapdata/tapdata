package io.tapdata;

import com.tapdata.tm.utils.OEMReplaceUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    }

    @Nested
    class getFileNameAfterOem {
        @Test
        void testNormal() {
            try (MockedStatic<OEMReplaceUtil> oem = org.mockito.Mockito.mockStatic(OEMReplaceUtil.class)) {
                oem.when(() -> OEMReplaceUtil.replace("fileName", "log/replace.json")).thenReturn("app");
                String fileName = Application.getFileNameAfterOem("fileName");
                Assertions.assertEquals("app", fileName);
            }
        }

        @Test
        void testNull() {
            try (MockedStatic<OEMReplaceUtil> oem = org.mockito.Mockito.mockStatic(OEMReplaceUtil.class)) {
                oem.when(() -> OEMReplaceUtil.replace("fileName", "log/replace.json")).thenReturn(null);
                String fileName = Application.getFileNameAfterOem("fileName");
                Assertions.assertEquals("fileName", fileName);
            }
        }
    }
}
package com.tapdata.tm.sdk.available;

import io.tapdata.utils.AppType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.CALLS_REAL_METHODS;


/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/4 17:43 Create
 */
public class TmStatusServiceTest {

    @Nested
    class IsEnable {

        @Test
        void testDAAS() {
            System.setProperty("TM_STATUS_SERVICE_ENABLE", "false");
            try {
                boolean result = TmStatusService.isEnable();
                Assertions.assertFalse(result, "Can't open TmStatus.");
            } finally {
                System.clearProperty("TM_STATUS_SERVICE_ENABLE");
            }
        }

        @Test
        void testDFS() {
            try (MockedStatic<AppType> mocked = Mockito.mockStatic(AppType.class, CALLS_REAL_METHODS)) {
                mocked.when(AppType::currentType).thenReturn(AppType.DFS);

                boolean result = TmStatusService.isEnable();
                Assertions.assertTrue(result, "Need to open TmStatus.");
            }
        }

        @Test
        void testNotFoundAppType() {
            System.setProperty("TM_STATUS_SERVICE_ENABLE", "false");
            try {
                boolean result = TmStatusService.isEnable();
                Assertions.assertFalse(result, "Can't open TmStatus.");
            } finally {
                System.clearProperty("TM_STATUS_SERVICE_ENABLE");
            }
        }
    }
}

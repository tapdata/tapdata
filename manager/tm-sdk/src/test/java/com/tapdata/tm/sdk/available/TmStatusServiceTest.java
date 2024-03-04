package com.tapdata.tm.sdk.available;

import com.tapdata.tm.sdk.util.AppType;
import com.tapdata.tm.sdk.util.AppTypeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/4 17:43 Create
 */
public class TmStatusServiceTest {

    public static <T> T callInType(Callable<T> callable, AppType appType, AppType... otherTypes) throws Exception {
        synchronized (TmStatusService.class) {
            Field field = TmStatusService.class.getDeclaredField("appType");
            field.setAccessible(true);
            field.set(null, null);
            return AppTypeTest.callInType(callable, appType, otherTypes);
        }
    }


    @Nested
    class IsEnable {
        @Test
        void testFalse() throws Exception {
            Boolean result = callInType(TmStatusService::isEnable, AppType.DAAS);
            Assertions.assertFalse(result, "Can't open TmStatus.");
        }

        @Test
        void testTrue() throws Exception {
            Boolean result = callInType(TmStatusService::isEnable, AppType.DFS);
            Assertions.assertTrue(result, "Need to open TmStatus.");
        }
    }
}

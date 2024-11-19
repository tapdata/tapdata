package io.tapdata.observable.logging;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/14 18:52
 */
public class BlankObsLoggerTest {

    @Test
    void testTrace() {
        BlankObsLogger blankObsLogger = new BlankObsLogger();
        Assertions.assertDoesNotThrow(() -> {
            blankObsLogger.trace(null, null);
        });
    }

}

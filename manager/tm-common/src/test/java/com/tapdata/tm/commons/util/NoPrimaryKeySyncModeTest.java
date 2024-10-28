package com.tapdata.tm.commons.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/10/28 10:33 Create
 */
public class NoPrimaryKeySyncModeTest {

    @Nested
    class fromValueTest {
        @Test
        void testBlank() {
            Assertions.assertEquals(NoPrimaryKeySyncMode.ALL_COLUMNS, NoPrimaryKeySyncMode.fromValue(null));
            Assertions.assertEquals(NoPrimaryKeySyncMode.ALL_COLUMNS, NoPrimaryKeySyncMode.fromValue(""));
        }

        @Test
        void testAllColumns() {
            Assertions.assertEquals(NoPrimaryKeySyncMode.ALL_COLUMNS, NoPrimaryKeySyncMode.fromValue("ALL_COLUMNS"));
        }

        @Test
        void testAddHash() {
            Assertions.assertEquals(NoPrimaryKeySyncMode.ADD_HASH, NoPrimaryKeySyncMode.fromValue("ADD_HASH"));
            Assertions.assertEquals(NoPrimaryKeySyncMode.ADD_HASH, NoPrimaryKeySyncMode.fromValue("others"));
        }
    }
}

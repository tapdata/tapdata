package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HistoryServiceTest {
    HistoryService service;
    @BeforeEach
    void init() {
        service = new HistoryService();
    }

    @Test
    void testList() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.list(null, null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testDeleteHookHistory() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.deleteHookHistory(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testReSend() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.reSend(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testPushManyHistory() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.pushManyHistory(null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testPushHistory() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.pushHistory(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
}
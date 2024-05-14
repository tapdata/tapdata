package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebHookDefaultServiceTest {
    WebHookDefaultService service;
    @BeforeEach
    void init() {
        service = new WebHookDefaultService();
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
    void testFindWebHookByHookId() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.findWebHookByHookId(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testCreate() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.create(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testUpdate() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.update(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testUpdatePingResult() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.updatePingResult(null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testClose() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.close(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testDelete() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.delete(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testFindMyOpenHookInfoList() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.findMyOpenHookInfoList(null, null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testCheckUrl() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.checkUrl(null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testPing() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.ping(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testReOpen() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.reOpen(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
}
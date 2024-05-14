package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdapterServiceTest {
    AdapterService service;
    @BeforeEach
    void init() {
        service = new AdapterService();
    }

    @Test
    void testSend() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.send(null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testSendAsync() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.sendAsync(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testSendAndSave() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.sendAndSave(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }

    @Test
    void testSend2() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.send(null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
}
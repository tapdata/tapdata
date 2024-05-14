package com.tapdata.tm.webhook.server.impl;


import com.tapdata.tm.webhook.enums.ConstVariable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpUtilServiceTest {
    HttpUtilService service;
    @BeforeEach
    void init() {
        service = new HttpUtilService();
    }

    @Test
    void testCheckURL() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.checkURL(null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testPost() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.post(null, null, null, null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
    @Test
    void testPost2() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> {
            try {
                service.post(null);
            } catch (UnsupportedOperationException e) {
                Assertions.assertEquals(ConstVariable.UN_SUPPORT_FUNCTION, e.getMessage());
                throw e;
            }
        });
    }
}
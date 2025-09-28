package com.tapdata.tm.system.api.vo;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DebugVoTest {

    @Test
    void testError() {
        DebugVo msg = DebugVo.error("msg");
        Assertions.assertNotNull(msg);
        Assertions.assertNotNull(msg.getError());
        Assertions.assertEquals("400", msg.getError().get("statusCode"));
        Assertions.assertEquals("BadRequestError", msg.getError().get("name"));
        Assertions.assertEquals("msg", msg.getError().get("message"));
        Assertions.assertEquals("INVALID_BODY_VALUE", msg.getError().get("code"));
    }

    @Test
    void testError2() {
        DebugVo msg = DebugVo.error(500, "msg");
        Assertions.assertNotNull(msg);
        Assertions.assertNotNull(msg.getError());
        Assertions.assertEquals(500, msg.getError().get("statusCode"));
        Assertions.assertEquals("BadRequestError", msg.getError().get("name"));
        Assertions.assertEquals("msg", msg.getError().get("message"));
    }
}
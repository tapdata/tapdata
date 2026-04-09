package com.tapdata.tm.modules.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class MongoUriUtilTest {

    @Nested
    class uriByParamTest {
        @Test
        void testNormal() {
            String string = MongoUriUtil.uriByParam(new HashMap<>());
            Assertions.assertEquals("mongodb://null:27017/null", string);
        }
        @Test
        void testNormal1() {
            Map<String, Object> config = new HashMap<>();
            config.put("host", "localhost");
            config.put("port", 80);
            config.put("additionalString", "id=i");
            String string = MongoUriUtil.uriByParam(config);
            Assertions.assertEquals("mongodb://localhost:80/null?id=i", string);
        }
        @Test
        void testNormal2() {
            Map<String, Object> config = new HashMap<>();
            config.put("host", "localhost");
            config.put("port", 80);
            config.put("additionalString", "id=i");
            config.put("user", "test");
            config.put("password", "password");
            String string = MongoUriUtil.uriByParam(config);
            Assertions.assertEquals("mongodb://test:password@localhost:80/null?id=i", string);
        }
    }

    @Nested
    class uriByConnectionStringTest {
        @Test
        void testNormal() {
            String uri = "xxx";
            Assertions.assertThrows(IllegalArgumentException.class, () -> MongoUriUtil.uriByConnectionString(uri));
        }
        @Test
        void testNormal1() {
            String uri = "mongodb://localhost/tapdata";
            Assertions.assertDoesNotThrow(() -> MongoUriUtil.uriByConnectionString(uri));
        }
        @Test
        void testNormal2() {
            String uri = "mongodb://admin:admin@localhost/tapdata";
            Assertions.assertDoesNotThrow(() -> MongoUriUtil.uriByConnectionString(uri));
        }
    }
}
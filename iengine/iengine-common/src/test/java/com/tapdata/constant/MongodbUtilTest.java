package com.tapdata.constant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongodbUtilTest {
    @DisplayName("test get mongoUri without additionalString")
    @Test
    void test1() {
        Map<String, Object> config = new HashMap<>();
        config.put("user", "testUser");
        config.put("password", "testPassword");
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        String mongoUri = MongodbUtil.getUri(config);
        assertEquals("mongodb://testUser:testPassword@localhost/testDatabase", mongoUri);
    }

    @DisplayName("test get mongoUri with additionalString")
    @Test
    void test2() {
        Map<String, Object> config = new HashMap<>();
        config.put("user", "testUser");
        config.put("password", "testPassword");
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        config.put("additionalString", "authSource=admin");
        String mongoUri = MongodbUtil.getUri(config);
        assertEquals("mongodb://testUser:testPassword@localhost/testDatabase?authSource=admin", mongoUri);
    }
    @DisplayName("test get mongoUri with user and password")
    @Test
    void test3(){
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("database", "testDatabase");
        config.put("additionalString", "authSource=admin");
        String mongoUri = MongodbUtil.getUri(config);
        assertEquals("mongodb://localhost/testDatabase?authSource=admin", mongoUri);
    }
    @DisplayName("test get MongoUri By Uri")
    @Test
    void test4(){
        Map<String, Object> config = new HashMap<>();
        String uri="mongodb://localhost/testDatabase?authSource=admin";
        config.put("isUri",true);
        config.put("uri",uri);
        String uriResult = MongodbUtil.getUri(config);
        assertEquals(uri,uriResult);
    }
}

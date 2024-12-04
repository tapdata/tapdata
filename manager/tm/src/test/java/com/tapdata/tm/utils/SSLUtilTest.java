package com.tapdata.tm.utils;

import com.mongodb.MongoClientSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SSLUtilTest {
    @Test
    void mongoClientSettingsTest() {
        MongoClientSettings settings = SSLUtil.mongoClientSettings(true, "src/test/resources/server.pem", "src/test/resources/ca.pem", "mongodb://127.0.0.1:27018");
        assertEquals(true, settings.getSslSettings().isEnabled());
    }

    @Test
    void mongoClientSettingsFalseTest() {
        MongoClientSettings settings = SSLUtil.mongoClientSettings(false, "src/test/resources/server.pem", "src/test/resources/ca.pem", "mongodb://127.0.0.1:27018");
        assertEquals(false, settings.getSslSettings().isEnabled());
    }
}
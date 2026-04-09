package com.tapdata.constant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Nested
    class MongoUriHelpersTest {
        @Test
        void testMaskUriPassword() {
            String uri = "mongodb://u:p%40ss@localhost/testDatabase?authSource=admin";
            String masked = MongodbUtil.maskUriPassword(uri);
            assertTrue(masked.contains("******@"));
            assertFalse(masked.contains("p%40ss@"));
        }

        @Test
        void testVerifyMongoDBUri() {
            assertThrows(IllegalArgumentException.class, () -> MongodbUtil.verifyMongoDBUri(""));
            assertNotNull(MongodbUtil.verifyMongoDBUri("mongodb://localhost/test"));
            assertThrows(IllegalArgumentException.class, () -> MongodbUtil.verifyMongoDBUri("not-a-uri"));
        }

        @Test
        void testVerifyMongoDBUriWithDB() {
            assertThrows(IllegalArgumentException.class, () -> MongodbUtil.verifyMongoDBUriWithDB("mongodb://localhost"));
            assertNotNull(MongodbUtil.verifyMongoDBUriWithDB("mongodb://localhost/test"));
        }

        @Test
        void testAppendMongoUri() {
            assertEquals("", MongodbUtil.appendMongoUri("", "u", "p".toCharArray()));
            assertEquals("mongodb://localhost:27017", MongodbUtil.appendMongoUri("localhost:27017", null, null));
            assertEquals("mongodb://u:p@localhost:27017", MongodbUtil.appendMongoUri("localhost:27017", "u", "p".toCharArray()));
            assertEquals("mongodb://localhost:27017/admin?serverSelectionTimeoutMS=1000",
                    MongodbUtil.appendMongoUri("localhost:27017", null, null, "1000", "admin"));
            assertEquals("mongodb://u:p@localhost:27017/admin?authSource=admin&serverSelectionTimeoutMS=1000",
                    MongodbUtil.appendMongoUri("localhost:27017", "u", "p".toCharArray(), "1000", "admin"));
        }
    }

    @Nested
    class PureStringTest {
        @Test
        void testReplicaSetUsedIn() {
            assertNull(MongodbUtil.replicaSetUsedIn("[::1]:27017"));
            assertNull(MongodbUtil.replicaSetUsedIn("localhost:27017"));
            assertEquals("rs0", MongodbUtil.replicaSetUsedIn("rs0/localhost:27017"));
        }

        @Test
        void testJoin() {
            assertEquals("", MongodbUtil.join(",", List.of()));
            assertEquals("null,a,b", MongodbUtil.join(",", Arrays.asList(null, "a", null, "b")));
            assertThrows(NullPointerException.class, () -> MongodbUtil.join(null, List.of("a")));
            assertThrows(NullPointerException.class, () -> MongodbUtil.join(",", null));
        }

        @Test
        void testSplitFilter() {
            String filterStr = "find({a:1},{b:1}).sort({c:-1}).limit(10).skip(5)";
            Map<String, String> m = MongodbUtil.splitFilter(filterStr);
            assertEquals("{a:1}", m.get("filter"));
            assertEquals("{b:1}", m.get("projection"));
            assertEquals("{c:-1}", m.get("sort"));
            assertEquals("10", m.get("limit"));
            assertEquals("5", m.get("skip"));
            assertNull(MongodbUtil.splitFilter(""));
        }
    }

    @Nested
    class MongodbKeyAndConditionTest {
        @Test
        void testMongodbKeySpecialCharHandler() {
            assertNull(MongodbUtil.mongodbKeySpecialCharHandler(null, "_"));
            assertEquals("", MongodbUtil.mongodbKeySpecialCharHandler("", "_"));
            assertEquals("_id", MongodbUtil.mongodbKeySpecialCharHandler("$id", "_"));
            assertEquals("a_b", MongodbUtil.mongodbKeySpecialCharHandler("a.b", "_"));
            assertEquals("a_b", MongodbUtil.mongodbKeySpecialCharHandler("a b", "_"));
            assertEquals("__tapd8.ts", MongodbUtil.mongodbKeySpecialCharHandler("__tapd8.ts", "_"));
        }

        @Test
        void testContainIdInConditionAndRemoveIdIfNeed() {
            List<Map<String, String>> joinCondition = new ArrayList<>();
            joinCondition.add(new HashMap<>(Map.of("t", "_id")));
            assertTrue(MongodbUtil.containIdInCondition(joinCondition));

            Map<String, Object> value = new HashMap<>();
            value.put("_id", 1);
            MongodbUtil.removeIdIfNeed(joinCondition, "", value);
            assertTrue(value.containsKey("_id"));

            List<Map<String, String>> joinCondition2 = new ArrayList<>();
            joinCondition2.add(new HashMap<>(Map.of("t", "id")));
            assertFalse(MongodbUtil.containIdInCondition(joinCondition2));
            Map<String, Object> value2 = new HashMap<>();
            value2.put("_id", 1);
            MongodbUtil.removeIdIfNeed(joinCondition2, "", value2);
            assertFalse(value2.containsKey("_id"));

            Map<String, Object> value3 = new HashMap<>();
            value3.put("_id", 1);
            MongodbUtil.removeIdIfNeed(joinCondition2, "path", value3);
            assertTrue(value3.containsKey("_id"));
        }
    }

    @Nested
    class ConnectionStringParseTest {
        @Test
        void testGetHostPortMap() {
            Map<String, String> m = MongodbUtil.getHostPortMap("mongodb://localhost:27018,example.com/test");
            List<String> hosts = Arrays.asList(m.get("hosts").split(","));
            List<String> ports = Arrays.asList(m.get("ports").split(","));
            assertEquals(2, hosts.size());
            assertEquals(2, ports.size());
            int localhostIndex = hosts.indexOf("localhost");
            int exampleIndex = hosts.indexOf("example.com");
            assertTrue(localhostIndex >= 0);
            assertTrue(exampleIndex >= 0);
            assertEquals("27018", ports.get(localhostIndex));
            assertEquals("27017", ports.get(exampleIndex));
        }
    }
}

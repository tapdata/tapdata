package com.tapdata.tm.config;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MongoCollectionStatisticsTest {
    @Test
    @DisplayName("旧版本统计：4和5保持原命令及返回类型")
    void legacyVersionsKeepOriginalDocument() {
        for (String version : Arrays.asList("4.4.30", "5.0.33")) {
            Fixture f = new Fixture(version);
            Document result = new MongoCollectionStatistics().read(f.database, "test");
            assertSame(f.legacy, result);
            assertEquals(Arrays.asList("buildInfo", "collStats"), f.commands);
        }
    }

    @Test
    @DisplayName("新版本统计：6至8映射字段并关闭游标")
    void modernVersionsKeepSingleResultFields() {
        for (String version : Arrays.asList("6.0.28", "7.0.43", "8.0.32")) {
            Fixture f = new Fixture(version);
            Document stats = stats(2, 42, 4096).append("capped", true)
                    .append("max", 100L).append("maxSize", 1048576L);
            f.rows.add(new Document("storageStats", stats));
            MongoCollectionStatistics reader = new MongoCollectionStatistics();
            assertEquals(stats, reader.read(f.database, "test"));
            assertTrue(f.closed);
            assertEquals(Arrays.asList("buildInfo", "aggregate"), f.commands);
            // A successful version decision is reused for this reader.
            reader.read(f.database, "test");
            assertEquals(Arrays.asList("buildInfo", "aggregate", "aggregate"), f.commands);
        }
    }

    @Test
    @DisplayName("多输出统计：合计大小和行数并加权平均")
    void multipleResultsAreSummed() {
        Fixture f = new Fixture("8.0.32");
        f.rows.add(new Document("storageStats", stats(2, 40, 4096)));
        f.rows.add(new Document("storageStats", stats(3L, 90L, 8192L)));
        Document result = new MongoCollectionStatistics().read(f.database, "test");
        assertEquals(5L, result.get("count"));
        assertEquals(130L, result.get("size"));
        assertEquals(12288L, result.get("storageSize"));
        assertEquals(26D, ((Number) result.get("avgObjSize")).doubleValue(), 0.000001);
    }

    @Test
    @DisplayName("版本查询失败：继续原统计查询")
    void versionFailureDoesNotAddPermissionPrerequisite() {
        Fixture f = new Fixture("8.0.32");
        f.versionFailure = new IllegalStateException("version unavailable");
        assertSame(f.legacy, new MongoCollectionStatistics().read(f.database, "test"));
    }

    @Test
    @DisplayName("统计权限失败：保留异常且不伪造零值")
    void statisticsFailurePropagatesAndClosesCursor() {
        Fixture f = new Fixture("8.0.32");
        f.queryFailure = error(13);
        assertSame(f.queryFailure, assertThrows(MongoCommandException.class,
                () -> new MongoCollectionStatistics().read(f.database, "test")));
        assertTrue(f.closed);
        assertFalse(f.commands.contains("collStats"));
    }

    @Test
    @DisplayName("集合不存在：保留服务器原统计契约")
    void missingCollectionUsesOriginalServerContract() {
        Fixture f = new Fixture("8.0.32");
        f.queryFailure = error(26);
        assertSame(f.legacy, new MongoCollectionStatistics().read(f.database, "test"));
        assertTrue(f.closed);
        assertEquals(Arrays.asList("buildInfo", "aggregate", "collStats"), f.commands);
    }

    @Test
    @DisplayName("连接切换：重新判断服务器版本")
    void reconnectDoesNotReuseAnotherServersVersion() {
        MongoCollectionStatistics reader = new MongoCollectionStatistics();
        Fixture oldServer = new Fixture("4.4.30");
        Fixture newServer = new Fixture("8.0.32");
        newServer.rows.add(new Document("storageStats", stats(2, 42, 4096)));
        reader.read(oldServer.database, "test");
        reader.read(newServer.database, "test");
        assertEquals(Arrays.asList("buildInfo", "aggregate"), newServer.commands);
    }

    @Test
    @DisplayName("同一连接不同数据库句柄：复用版本查询")
    void databaseHandlesShareConnectionVersion() {
        MongoCollectionStatistics reader = new MongoCollectionStatistics();
        Object connection = new Object();
        Fixture first = new Fixture("8.0.32");
        Fixture second = new Fixture("8.0.32");
        first.rows.add(new Document("storageStats", stats(2, 42, 4096)));
        second.rows.add(new Document("storageStats", stats(2, 42, 4096)));
        reader.read(first.database, "test", connection);
        reader.read(second.database, "test", connection);
        assertEquals(Arrays.asList("aggregate"), second.commands);
    }

    private static Document stats(Number count, Number size, Number storageSize) {
        return new Document("count", count).append("size", size).append("storageSize", storageSize)
                .append("avgObjSize", count.longValue() == 0 ? 0D : size.doubleValue() / count.doubleValue());
    }

    private static MongoCommandException error(int code) {
        return new MongoCommandException(new BsonDocument("code", new BsonInt32(code))
                .append("errmsg", new BsonString("fixture error")), new ServerAddress());
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static class Fixture {
        final List<String> commands = new ArrayList<>();
        final List<Document> rows = new ArrayList<>();
        final Document legacy = stats(2, 42, 4096);
        final MongoDatabase database;
        RuntimeException versionFailure;
        RuntimeException queryFailure;
        boolean closed;

        Fixture(String version) {
            database = proxy(MongoDatabase.class, (p, m, args) -> {
                if ("runCommand".equals(m.getName())) {
                    Document command = (Document) args[0];
                    String name = command.keySet().iterator().next();
                    commands.add(name);
                    if ("buildInfo".equals(name)) {
                        if (versionFailure != null) throw versionFailure;
                        return new Document("version", version);
                    }
                    return legacy;
                }
                if ("getCollection".equals(m.getName())) {
                    return proxy(MongoCollection.class, (cp, cm, ca) -> {
                        assertEquals("aggregate", cm.getName());
                        assertEquals(Arrays.asList(new Document("$collStats",
                                new Document("storageStats", new Document()))), ca[0]);
                        commands.add("aggregate");
                        return proxy(AggregateIterable.class, (ap, am, aa) -> {
                            assertEquals("iterator", am.getName());
                            Iterator<Document> iterator = rows.iterator();
                            return proxy(MongoCursor.class, (ip, im, ia) -> {
                                if ("close".equals(im.getName())) { closed = true; return null; }
                                if (queryFailure != null) throw queryFailure;
                                if ("hasNext".equals(im.getName())) return iterator.hasNext();
                                if ("next".equals(im.getName())) return iterator.next();
                                throw new UnsupportedOperationException(im.getName());
                            });
                        });
                    });
                }
                throw new UnsupportedOperationException(m.getName());
            });
        }
    }
}


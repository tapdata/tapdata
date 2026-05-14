package com.tapdata.tm.v2.api.pool.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import com.tapdata.tm.worker.entity.field.ConnectionPoolField;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionPoolInstanceFactoryTest {

    static class TestFactory extends ConnectionPoolInstanceFactory {
        public TestFactory(Consumer<List<ConnectionPoolEntity>> consumer, Function<Query, ConnectionPoolEntity> findOne) {
            super(consumer, findOne);
        }
    }

    private static Document doc(String serverId, String connectionId, long lastUpdateMs) {
        return new Document()
                .append(ConnectionPoolField.PROCESS_ID.field(), serverId)
                .append(ConnectionPoolField.CONNECTION_ID.field(), connectionId)
                .append(ConnectionPoolField.LAST_UPDATE_TIME.field(), lastUpdateMs)
                .append(ConnectionPoolField.MAX_CONNECTIONS.field(), 10)
                .append(ConnectionPoolField.USED_CONNECTIONS.field(), 3)
                .append(ConnectionPoolField.QUEUE_SIZE.field(), 1);
    }

    @Test
    void testAcceptNull() {
        AtomicInteger consumed = new AtomicInteger(0);
        TestFactory factory = new TestFactory(list -> consumed.addAndGet(list.size()), q -> null);
        factory.accept(null);
        assertTrue(factory.getApiMetricsRaws().isEmpty());
        assertTrue(factory.getInstanceMap().isEmpty());
        assertEquals(0, consumed.get());
        factory.close();
        assertEquals(0, consumed.get());
    }

    @Test
    void testComputeIfAbsentLastMinNull() {
        AtomicInteger findCalls = new AtomicInteger(0);
        TestFactory factory = new TestFactory(list -> {
        }, q -> {
            findCalls.incrementAndGet();
            return null;
        });

        factory.accept(doc("s", "c", 120_000L));
        assertTrue(factory.needUpdate());
        assertEquals(1, factory.getInstanceMap().size());
        assertEquals(1, findCalls.get());
    }

    @Test
    void testComputeIfAbsentLastMinNotNullAndHourFetched() {
        AtomicInteger minuteCalls = new AtomicInteger(0);
        AtomicInteger hourCalls = new AtomicInteger(0);
        AtomicReference<Long> hourGte = new AtomicReference<>(null);

        TestFactory factory = new TestFactory(list -> {
        }, q -> {
            Document qo = q.getQueryObject();
            int type = ((Number) qo.get(ConnectionPoolField.TIME_GRANULARITY.field())).intValue();
            if (type == TimeGranularity.MINUTE.getType()) {
                minuteCalls.incrementAndGet();
                ConnectionPoolEntity lastMin = new ConnectionPoolEntity();
                lastMin.setLastUpdateTime(TimeGranularity.MINUTE.fixTime(120L) * 1000L);
                lastMin.setProcessId(qo.getString(ConnectionPoolField.PROCESS_ID.field()));
                lastMin.setConnectionId(qo.getString(ConnectionPoolField.CONNECTION_ID.field()));
                lastMin.setTimeGranularity(type);
                return lastMin;
            }
            if (type == TimeGranularity.HOUR.getType()) {
                hourCalls.incrementAndGet();
                Document lastUpdateTime = (Document) qo.get(ConnectionPoolField.LAST_UPDATE_TIME.field());
                hourGte.set(((Number) lastUpdateTime.get("$gte")).longValue());
                return null;
            }
            return null;
        });

        factory.accept(doc("s", "c", 120_000L));
        assertEquals(1, minuteCalls.get());
        assertEquals(1, hourCalls.get());
        assertNotNull(hourGte.get());
    }

    @Test
    void testLastOneTimeStartNullAndNotNull() {
        List<Query> captured = new ArrayList<>();
        TestFactory factory = new TestFactory(list -> {
        }, q -> {
            captured.add(q);
            return null;
        });

        assertNull(factory.lastOne("s", "c", TimeGranularity.MINUTE.getType(), null));
        assertNull(factory.lastOne("s", "c", TimeGranularity.HOUR.getType(), 10_000L));
        assertEquals(2, captured.size());

        Document q1 = captured.get(0).getQueryObject();
        assertNull(q1.get(ConnectionPoolField.LAST_UPDATE_TIME.field()));

        Document q2 = captured.get(1).getQueryObject();
        Document lastUpdateTime = (Document) q2.get(ConnectionPoolField.LAST_UPDATE_TIME.field());
        assertNotNull(lastUpdateTime);
        assertEquals(10_000L, ((Number) lastUpdateTime.get("$gte")).longValue());
    }

    @Test
    void testFlushWhenReachBatchSizeAndCloseFlushRemaining() {
        String serverId = "server-id";
        String connectionId = new ObjectId().toHexString();
        AtomicInteger consumerCalls = new AtomicInteger(0);
        AtomicInteger consumerSize = new AtomicInteger(0);

        TestFactory factory = new TestFactory(list -> {
            consumerCalls.incrementAndGet();
            consumerSize.set(list.size());
        }, q -> null);

        long baseMs = 120L * 1000L;
        for (int i = 0; i < 101; i++) {
            factory.accept(doc(serverId, connectionId, baseMs + i * 60_000L));
        }

        assertEquals(1, consumerCalls.get());
        assertEquals(100, consumerSize.get());
        assertTrue(factory.getApiMetricsRaws().size() < 100);

        factory.close();
        assertEquals(2, consumerCalls.get());
    }
}

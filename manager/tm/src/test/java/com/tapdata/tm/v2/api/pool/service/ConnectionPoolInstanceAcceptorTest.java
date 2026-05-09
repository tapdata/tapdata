package com.tapdata.tm.v2.api.pool.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import com.tapdata.tm.worker.entity.field.ConnectionPoolField;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionPoolInstanceAcceptorTest {

    @Test
    void testAcceptNull() {
        List<ConnectionPoolEntity> consumed = new ArrayList<>();
        ConnectionPoolInstanceAcceptor acceptor = new ConnectionPoolInstanceAcceptor(null, null, consumed::add);
        acceptor.accept(null);
        acceptor.close();
        assertTrue(consumed.isEmpty());
    }

    @Test
    void testAcceptAvg() {
        ConnectionPoolInstanceAcceptor acceptor = new ConnectionPoolInstanceAcceptor(null, null, e -> {
        });

        AtomicInteger v = new AtomicInteger(-1);
        acceptor.acceptAvg(new ArrayList<>(), v::set);
        assertEquals(-1, v.get());

        List<Number> numbers = new ArrayList<>();
        numbers.add(null);
        numbers.add(0);
        numbers.add(5);
        numbers.add(15);
        acceptor.acceptAvg(numbers, v::set);
        assertEquals(10, v.get());
    }

    @Test
    void testBucketChangeAndClose() {
        List<ConnectionPoolEntity> consumed = new ArrayList<>();
        ConnectionPoolInstanceAcceptor acceptor = new ConnectionPoolInstanceAcceptor(null, null, consumed::add);

        String serverId = "server-id";
        String connectionId = "connection-id";
        long baseSeconds = 120L;
        long baseMs = baseSeconds * 1000L;

        Document d1 = new Document()
                .append(ConnectionPoolField.PROCESS_ID.field(), serverId)
                .append(ConnectionPoolField.CONNECTION_ID.field(), connectionId)
                .append(ConnectionPoolField.LAST_UPDATE_TIME.field(), baseMs)
                .append(ConnectionPoolField.MAX_CONNECTIONS.field(), 10)
                .append(ConnectionPoolField.USED_CONNECTIONS.field(), 5)
                .append(ConnectionPoolField.QUEUE_SIZE.field(), 2);
        acceptor.accept(d1);

        Document d2 = new Document()
                .append(ConnectionPoolField.PROCESS_ID.field(), serverId)
                .append(ConnectionPoolField.CONNECTION_ID.field(), connectionId)
                .append(ConnectionPoolField.LAST_UPDATE_TIME.field(), baseMs + 60_000L)
                .append(ConnectionPoolField.MAX_CONNECTIONS.field(), 20)
                .append(ConnectionPoolField.USED_CONNECTIONS.field(), 10)
                .append(ConnectionPoolField.QUEUE_SIZE.field(), 4);
        acceptor.accept(d2);

        assertEquals(1, consumed.size());
        ConnectionPoolEntity minute1 = consumed.get(0);
        assertEquals(TimeGranularity.MINUTE.getType(), minute1.getTimeGranularity());
        assertEquals(TimeGranularity.MINUTE.fixTime(baseSeconds) * 1000L, minute1.getLastUpdateTime());
        assertEquals(10, minute1.getMaxConnections());
        assertEquals(5, minute1.getUsedConnections());
        assertEquals(2, minute1.getQueueSize());

        acceptor.close();

        assertEquals(3, consumed.size());
        ConnectionPoolEntity minute2 = consumed.get(1);
        assertEquals(TimeGranularity.MINUTE.getType(), minute2.getTimeGranularity());
        assertEquals(TimeGranularity.MINUTE.fixTime(baseSeconds + 60L) * 1000L, minute2.getLastUpdateTime());
        assertEquals(20, minute2.getMaxConnections());
        assertEquals(10, minute2.getUsedConnections());
        assertEquals(4, minute2.getQueueSize());

        ConnectionPoolEntity hour = consumed.get(2);
        assertEquals(TimeGranularity.HOUR.getType(), hour.getTimeGranularity());
        assertEquals(TimeGranularity.HOUR.fixTime(baseSeconds) * 1000L, hour.getLastUpdateTime());
        assertEquals(15, hour.getMaxConnections());
        assertEquals(7, hour.getUsedConnections());
        assertEquals(3, hour.getQueueSize());
    }

    @Test
    void testTimeShiftTriggersEarlyAccept() {
        List<ConnectionPoolEntity> consumed = new ArrayList<>();
        ConnectionPoolEntity lastMin = new ConnectionPoolEntity();
        lastMin.setLastUpdateTime(0L);
        lastMin.setTimeGranularity(TimeGranularity.MINUTE.getType());
        ConnectionPoolEntity lastHour = new ConnectionPoolEntity();
        lastHour.setLastUpdateTime(0L);
        lastHour.setTimeGranularity(TimeGranularity.HOUR.getType());

        ConnectionPoolInstanceAcceptor acceptor = new ConnectionPoolInstanceAcceptor(lastMin, lastHour, consumed::add);

        long tsMs = (3600L + 120L) * 1000L;
        Document d = new Document()
                .append(ConnectionPoolField.PROCESS_ID.field(), "server-id")
                .append(ConnectionPoolField.CONNECTION_ID.field(), "connection-id")
                .append(ConnectionPoolField.LAST_UPDATE_TIME.field(), tsMs);
        Assertions.assertDoesNotThrow(() -> acceptor.accept(d));

        assertEquals(2, consumed.size());
        assertNotNull(consumed.get(0));
        assertNotNull(consumed.get(1));
    }
}


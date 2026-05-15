package com.tapdata.tm.v2.api.usage.service;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.field.ServerUsageField;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerUsageMetricInstanceAcceptorTest {

    private static Document doc(String serverId, Object workOid, long lastUpdateMs, long heapUsage, long heapMax, double cpu, int poolMax, int poolUsed, int poolQueue) {
        Document d = new Document()
                .append(ServerUsageField.PROCESS_ID.field(), serverId)
                .append(ServerUsageField.LAST_UPDATE_TIME.field(), lastUpdateMs)
                .append(ServerUsageField.HEAP_MEMORY_USAGE.field(), heapUsage)
                .append(ServerUsageField.HEAP_MEMORY_MAX.field(), heapMax)
                .append(ServerUsageField.CPU_USAGE.field(), cpu)
                .append(ServerUsageField.POOL_MAX_CONNECTIONS.field(), poolMax)
                .append(ServerUsageField.POOL_USED_CONNECTIONS.field(), poolUsed)
                .append(ServerUsageField.POOL_QUEUE_SIZE.field(), poolQueue);
        if (workOid != null) {
            d.append(ServerUsageField.WORK_OID.field(), workOid);
        }
        return d;
    }

    @Test
    void testAcceptNull() {
        List<ServerUsageMetric> consumed = new ArrayList<>();
        ServerUsageMetricInstanceAcceptor acceptor = new ServerUsageMetricInstanceAcceptor(null, null, consumed::add);
        acceptor.accept(null);
        acceptor.close();
        assertTrue(consumed.isEmpty());
    }

    @Test
    void testAcceptMemoryMemMaxCpuAvg() {
        ServerUsageMetricInstanceAcceptor acceptor = new ServerUsageMetricInstanceAcceptor(null, null, e -> {
        });

        ServerUsageMetric usage = new ServerUsageMetric();

        acceptor.acceptMemMax(usage, new ArrayList<>());
        acceptor.acceptMemory(usage, new ArrayList<>());
        acceptor.acceptCpu(usage, new ArrayList<>());
        assertNull(usage.getHeapMemoryMax());
        assertNull(usage.getHeapMemoryUsage());
        assertNull(usage.getCpuUsage());

        List<Long> mem = List.of(10L, 30L, 20L);
        List<Long> memMax = List.of(100L, 300L);
        List<Double> cpu = List.of(0.5D, 0.2D, 0.9D);
        acceptor.acceptMemory(usage, mem);
        acceptor.acceptMemMax(usage, memMax);
        acceptor.acceptCpu(usage, cpu);

        assertEquals(10L, usage.getMinHeapMemoryUsage());
        assertEquals(30L, usage.getMaxHeapMemoryUsage());
        assertEquals(20L, usage.getHeapMemoryUsage());
        assertEquals(200L, usage.getHeapMemoryMax());
        assertEquals(0.2D, usage.getMinCpuUsage());
        assertEquals(0.9D, usage.getMaxCpuUsage());
        assertEquals(0.5333333333333333D, usage.getCpuUsage());

        AtomicInteger avg = new AtomicInteger(-1);
        acceptor.acceptAvg(new ArrayList<>(), avg::set);
        assertEquals(-1, avg.get());
        List<Number> numbers = new ArrayList<>();
        numbers.add(null);
        numbers.add(0);
        numbers.add(5);
        numbers.add(15);
        acceptor.acceptAvg(numbers, avg::set);
        assertEquals(10, avg.get());
    }

    @Test
    void testBucketChangeAndProcessType() {
        List<ServerUsageMetric> consumed = new ArrayList<>();
        ServerUsageMetricInstanceAcceptor acceptor = new ServerUsageMetricInstanceAcceptor(null, null, consumed::add);

        String serverId = "server-id";
        long baseSeconds = 120L;
        long baseMs = baseSeconds * 1000L;

        acceptor.accept(doc(serverId, null, baseMs, 10, 100, 0.1D, 10, 3, 1));
        acceptor.accept(doc(serverId, new ObjectId(), baseMs + 60_000L, 20, 200, 0.2D, 20, 6, 2));

        assertEquals(1, consumed.size());
        assertEquals(TimeGranularity.MINUTE.getType(), consumed.get(0).getTimeGranularity());
        assertEquals(TimeGranularity.MINUTE.fixTime(baseSeconds) * 1000L, consumed.get(0).getLastUpdateTime());
        assertEquals(ServerUsage.ProcessType.API_SERVER.getType(), consumed.get(0).getProcessType());

        acceptor.close();
        assertEquals(3, consumed.size());

        assertEquals(ServerUsage.ProcessType.API_SERVER_WORKER.getType(), consumed.get(1).getProcessType());
        assertEquals(TimeGranularity.HOUR.getType(), consumed.get(2).getTimeGranularity());
        assertNotNull(consumed.get(2).getPoolMaxConnections());
    }

    @Test
    void testTimeShiftTriggersEarlyAccept() {
        List<ServerUsageMetric> consumed = new ArrayList<>();

        ServerUsageMetric lastMin = ServerUsageMetric.instance(TimeGranularity.MINUTE.getType(), 0L, "s", null, ServerUsage.ProcessType.API_SERVER.getType());
        ServerUsageMetric lastHour = ServerUsageMetric.instance(TimeGranularity.HOUR.getType(), 0L, "s", null, ServerUsage.ProcessType.API_SERVER.getType());

        ServerUsageMetricInstanceAcceptor acceptor = new ServerUsageMetricInstanceAcceptor(lastMin, lastHour, consumed::add);
        long tsMs = (3600L + 120L) * 1000L;
        Document d = new Document()
                .append(ServerUsageField.PROCESS_ID.field(), "s")
                .append(ServerUsageField.LAST_UPDATE_TIME.field(), tsMs);

        Assertions.assertDoesNotThrow(() -> acceptor.accept(d));
        assertEquals(2, consumed.size());
    }

    @Test
    void testPushDefaultValue() {
        List<ServerUsageMetric> consumed = new ArrayList<>();
        ServerUsageMetricInstanceAcceptor acceptor = new ServerUsageMetricInstanceAcceptor(null, null, consumed::add);

        String serverId = "server-id";
        long baseMs = 120_000L;
        Document d1 = new Document()
                .append(ServerUsageField.PROCESS_ID.field(), serverId)
                .append(ServerUsageField.LAST_UPDATE_TIME.field(), baseMs);
        Document d2 = new Document()
                .append(ServerUsageField.PROCESS_ID.field(), serverId)
                .append(ServerUsageField.LAST_UPDATE_TIME.field(), baseMs + 60_000L);

        acceptor.accept(d1);
        acceptor.accept(d2);
        assertEquals(1, consumed.size());
        ServerUsageMetric m = consumed.get(0);
        assertNotNull(m);
        assertNotNull(m.getHeapMemoryUsage());
    }
}

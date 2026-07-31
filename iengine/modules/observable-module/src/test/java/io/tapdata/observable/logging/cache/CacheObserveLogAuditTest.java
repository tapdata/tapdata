package io.tapdata.observable.logging.cache;

import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class CacheObserveLogAuditTest {

    @Test
    void shouldWarnWithStableFieldsWhenUnconsumedLogsAreDeleted() {
        Logger logger = mock(Logger.class);
        CacheObserveLogAudit audit = new CacheObserveLogAudit(logger);

        audit.evict("task-id", "task-name", 7L, true, "segment-7", 100L, 200L);

        verify(logger).warn(
                eq("CACHE_OBSERVE_LOG_EVICT taskId={} taskName={} generation={} file={} "
                        + "dataLoss={} taskCacheBytes={} totalCacheBytes={}"),
                eq("task-id"),
                eq("task-name"),
                eq(7L),
                eq("segment-7"),
                eq(true),
                eq(100L),
                eq(200L));
        verify(logger, never()).info(anyString(), any(Object[].class));
    }

    @Test
    void shouldUseInfoWhenDeletedLogsWereFullyConsumed() {
        Logger logger = mock(Logger.class);
        CacheObserveLogAudit audit = new CacheObserveLogAudit(logger);

        audit.evict("task-id", "task-name", 7L, false, "segment-7", 100L, 200L);

        verify(logger).info(
                eq("CACHE_OBSERVE_LOG_EVICT taskId={} taskName={} generation={} file={} "
                        + "dataLoss={} taskCacheBytes={} totalCacheBytes={}"),
                eq("task-id"),
                eq("task-name"),
                eq(7L),
                eq("segment-7"),
                eq(false),
                eq(100L),
                eq(200L));
        verify(logger, never()).warn(anyString(), any(Object[].class));
    }

    @Test
    void shouldWarnWhenIngressQueueRejectsLog() {
        Logger logger = mock(Logger.class);
        CacheObserveLogAudit audit = new CacheObserveLogAudit(logger);

        audit.writeRejected("task-id", "task-name", "WARN", 3L, 100, 100);

        verify(logger).warn(
                eq("CACHE_OBSERVE_LOG_WRITE_REJECTED taskId={} taskName={} level={} "
                        + "reason=ingress_queue_full dropped={} queueSize={} queueCapacity={}"),
                eq("task-id"),
                eq("task-name"),
                eq("WARN"),
                eq(3L),
                eq(100),
                eq(100));
    }
}

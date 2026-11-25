package io.tapdata.inspect;

import com.tapdata.tm.commons.inspect.*;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CustomQueryRunnerReviewTest {
    private ConnectorNode node(String id, ExecuteCommandFunction function) {
        ConnectorNode node = mock(ConnectorNode.class);
        ConnectorFunctions functions = mock(ConnectorFunctions.class);
        when(functions.getExecuteCommandFunction()).thenReturn(function);
        when(node.getConnectorFunctions()).thenReturn(functions);
        TapConnectorContext context = mock(TapConnectorContext.class, RETURNS_DEEP_STUBS);
        when(context.getSpecification().getId()).thenReturn(id);
        when(node.getConnectorContext()).thenReturn(context);
        doAnswer(call -> { ((Runnable) call.getArgument(0)).run(); return null; }).when(node).applyClassLoaderContext(any());
        return node;
    }
    private CustomQueryRequest request() {
        CustomQueryRequest request = new CustomQueryRequest();
        request.setConnectionId("connection"); request.setQuery("select 1"); request.setTimeoutSeconds(10);
        return request;
    }
    @Test void paimonCountAndRowSetsRemainSupported() {
        for (Object result : List.of(42L, List.of(Collections.singletonMap("value", 42L)))) {
            AtomicInteger cleanups = new AtomicInteger();
            ConnectorNode node = node("Paimon", (ctx, cmd, consumer) ->
                consumer.accept(new ExecuteResult<>().result(result)));
            CustomQueryConfig.SideSummary summary = new CustomQueryConfig.SideSummary();
            CustomQueryConfig.Snapshot snapshot = CustomQueryRunner.execute(
                node, request(), 20, true, summary, cleanups::incrementAndGet);
            assertEquals(List.of(Collections.singletonMap("value", 42L)), snapshot.getRows());
            assertEquals("success", summary.getStatus());
            assertEquals(1, cleanups.get());
        }
    }
    @Test void saturationFinalizesSummaryAndCleansUpRejectedInvocationExactlyOnce() throws Exception {
        CountDownLatch occupied = new CountDownLatch(16), release = new CountDownLatch(1);
        ExecutorService callers = Executors.newFixedThreadPool(16);
        List<Future<?>> running = new ArrayList<>();
        ConnectorNode blocking = node("postgres", (ctx, cmd, consumer) -> { occupied.countDown(); release.await(); });
        try {
            for (int i = 0; i < 16; i++) running.add(callers.submit(() ->
                CustomQueryRunner.execute(blocking, request(), 20, true, new CustomQueryConfig.SideSummary())));
            assertTrue(occupied.await(5, TimeUnit.SECONDS));
            AtomicInteger cleanups = new AtomicInteger();
            CustomQueryConfig.SideSummary summary = new CustomQueryConfig.SideSummary();
            RejectedExecutionException error = assertThrows(RejectedExecutionException.class, () ->
                CustomQueryRunner.execute(blocking, request(), 20, true, summary, cleanups::incrementAndGet));
            assertTrue(error.getMessage().contains("capacity"));
            assertEquals("error", summary.getStatus());
            assertTrue(summary.getEndedAt() >= summary.getStartedAt());
            assertEquals(summary.getEndedAt() - summary.getStartedAt(), summary.getDurationMillis());
            assertEquals(1, cleanups.get());
        } finally {
            release.countDown();
            // Empty rows intentionally lack schema; these accepted invocations may fail on completion.
            for (Future<?> future : running) try { future.get(3, TimeUnit.SECONDS); } catch (ExecutionException expected) { }
            callers.shutdownNow(); assertTrue(callers.awaitTermination(3, TimeUnit.SECONDS));
        }
    }
}

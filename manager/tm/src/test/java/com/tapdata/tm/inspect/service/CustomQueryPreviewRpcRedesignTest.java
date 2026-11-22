package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import io.tapdata.modules.api.net.service.EngineMessageExecutionService;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CustomQueryPreviewRpcRedesignTest {
    private CustomQueryRequest request() {
        CustomQueryRequest query=new CustomQueryRequest();query.setConnectionId("c");query.setQuery("select 1 v");query.setTimeoutSeconds(60);return query;
    }
    private CustomQueryPreviewRpc rpc(EngineMessageExecutionService execution) {
        return new CustomQueryPreviewRpc() {
            @Override protected EngineMessageExecutionService executionService() {return execution;}
        };
    }
    @Test void configuredPreviewCanReturnAfterTheLegacyThirtySecondDeadline() throws Throwable {
        EngineMessageExecutionService execution=mock(EngineMessageExecutionService.class);
        ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor();
        CustomQueryConfig.Snapshot expected=new CustomQueryConfig.Snapshot();
        doAnswer(call->{
            BiConsumer<Object,Throwable> callback=call.getArgument(1);
            scheduler.schedule(()->callback.accept(expected,null),31,TimeUnit.SECONDS);return null;
        }).when(execution).call(any(),any());
        try {assertSame(expected,rpc(execution).test(request()));} finally {scheduler.shutdownNow();}
    }
    @Test void engineFailuresPropagateAndUnavailableEngineFailsImmediately() throws Throwable {
        EngineMessageExecutionService execution=mock(EngineMessageExecutionService.class);
        IllegalStateException failure=new IllegalStateException("Query deadline exceeded");
        doAnswer(call->{BiConsumer<Object,Throwable> callback=call.getArgument(1);callback.accept(null,failure);return null;}).when(execution).call(any(),any());
        assertSame(failure,assertThrows(IllegalStateException.class,()->rpc(execution).test(request())));
        assertThrows(IllegalStateException.class,()->rpc(null).test(request()));
    }
    @Test void asyncPreviewDoesNotBlockAndLimitsConcurrentRequestsPerUser() throws Exception {
        EngineMessageExecutionService execution = mock(EngineMessageExecutionService.class);
        BlockingQueue<BiConsumer<Object,Throwable>> callbacks = new LinkedBlockingQueue<>();
        doAnswer(call -> { callbacks.add(call.getArgument(1)); return null; }).when(execution).call(any(),any());
        CustomQueryPreviewRpc rpc = rpc(execution);
        CompletableFuture<CustomQueryConfig.Snapshot> first = rpc.testAsync(request(), "alice");
        CompletableFuture<CustomQueryConfig.Snapshot> second = rpc.testAsync(request(), "alice");
        assertFalse(first.isDone()); assertFalse(second.isDone());
        assertThrows(RejectedExecutionException.class, () -> rpc.testAsync(request(), "alice"));
        CompletableFuture<CustomQueryConfig.Snapshot> otherUser = rpc.testAsync(request(), "bob");
        for (int i=0; i<3; i++) {
            BiConsumer<Object,Throwable> callback = callbacks.poll(2,TimeUnit.SECONDS);
            assertNotNull(callback); callback.accept(new CustomQueryConfig.Snapshot(),null);
        }
        first.get(2,TimeUnit.SECONDS); second.get(2,TimeUnit.SECONDS); otherUser.get(2,TimeUnit.SECONDS);
        CompletableFuture<CustomQueryConfig.Snapshot> retry = rpc.testAsync(request(), "alice");
        callbacks.poll(2,TimeUnit.SECONDS).accept(new CustomQueryConfig.Snapshot(),null);
        assertNotNull(retry.get(2,TimeUnit.SECONDS));
    }

}

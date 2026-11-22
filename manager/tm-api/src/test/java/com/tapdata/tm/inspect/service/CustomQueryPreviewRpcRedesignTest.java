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
}

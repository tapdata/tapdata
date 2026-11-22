package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.service.EngineMessageExecutionService;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import org.springframework.stereotype.Service;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Preview RPC budget includes the configured query deadline and time for setup/cleanup. */
@Service
public class CustomQueryPreviewRpc {
    protected EngineMessageExecutionService executionService() {
        return InstanceFactory.instance(EngineMessageExecutionService.class, true);
    }

    public CustomQueryConfig.Snapshot test(CustomQueryRequest request) throws Throwable {
        request.validate();
        EngineMessageExecutionService execution = executionService();
        if (execution == null) throw new IllegalStateException("Engine execution service is unavailable");
        ServiceCaller caller = ServiceCaller.create(UUID.randomUUID().toString()).className("CustomQueryPreviewService")
                .method("test").args(new Object[]{request});
        caller.setReturnClass(CustomQueryConfig.Snapshot.class.getName());
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<CustomQueryConfig.Snapshot> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        execution.call(caller, (data, failure) -> {
            if (failure != null) error.set(failure);
            else if (data instanceof CustomQueryConfig.Snapshot) result.set((CustomQueryConfig.Snapshot) data);
            else error.set(new IllegalStateException("Invalid custom query preview response"));
            completed.countDown();
        });
        long timeout = request.getTimeoutSeconds() + 30L;
        if (!completed.await(timeout, TimeUnit.SECONDS))
            throw new TimeoutException("Custom query preview RPC timed out after " + timeout + " seconds");
        if (error.get() != null) throw error.get();
        return result.get();
    }
}

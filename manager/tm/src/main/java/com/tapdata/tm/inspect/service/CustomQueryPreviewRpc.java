package com.tapdata.tm.inspect.service;

import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.service.EngineMessageExecutionService;
import io.tapdata.pdk.apis.entity.message.ServiceCaller;
import org.springframework.stereotype.Service;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Asynchronous preview RPC with bounded dispatch and at most two outstanding requests per user. */
@Service
public class CustomQueryPreviewRpc {
    private static final ExecutorService DISPATCH = new ThreadPoolExecutor(0, 16, 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> { Thread t = new Thread(r, "inspect-preview-rpc"); t.setDaemon(true); return t; });
    private static final ScheduledThreadPoolExecutor DEADLINES = new ScheduledThreadPoolExecutor(1,
            r -> { Thread t = new Thread(r, "inspect-preview-deadline"); t.setDaemon(true); return t; });
    static { DEADLINES.setRemoveOnCancelPolicy(true); }
    private final ConcurrentHashMap<String, Integer> activeUsers = new ConcurrentHashMap<>();
    private final Semaphore capacity = new Semaphore(16);

    protected EngineMessageExecutionService executionService() {
        return InstanceFactory.instance(EngineMessageExecutionService.class, true);
    }

    public CompletableFuture<CustomQueryConfig.Snapshot> testAsync(CustomQueryRequest request, String userId) {
        request.validate();
        if (userId == null || userId.isEmpty()) throw new IllegalArgumentException("Authenticated user is required");
        AtomicBoolean acquired = new AtomicBoolean();
        activeUsers.compute(userId, (key, count) -> {
            int current = count == null ? 0 : count;
            if (current >= 2 || !capacity.tryAcquire()) return count;
            acquired.set(true); return current + 1;
        });
        if (!acquired.get()) throw new RejectedExecutionException("Too many concurrent custom query previews; try again later");
        CompletableFuture<CustomQueryConfig.Snapshot> result = new CompletableFuture<>();
        long timeout = request.getTimeoutSeconds() + 30L;
        ScheduledFuture<?> deadline = DEADLINES.schedule(() -> result.completeExceptionally(
                new TimeoutException("Custom query preview RPC timed out after " + timeout + " seconds")), timeout, TimeUnit.SECONDS);
        result.whenComplete((data, failure) -> {
            deadline.cancel(false);
            activeUsers.computeIfPresent(userId, (key, count) -> count == 1 ? null : count - 1);
            capacity.release();
        });
        try {
            DISPATCH.execute(() -> {
                try {
                    EngineMessageExecutionService execution = executionService();
                    if (execution == null) throw new IllegalStateException("Engine execution service is unavailable");
                    ServiceCaller caller = ServiceCaller.create(UUID.randomUUID().toString()).className("CustomQueryPreviewService")
                            .method("test").args(new Object[]{request});
                    caller.setReturnClass(CustomQueryConfig.Snapshot.class.getName());
                    execution.call(caller, (data, failure) -> {
                        if (failure != null) result.completeExceptionally(failure);
                        else if (data instanceof CustomQueryConfig.Snapshot) result.complete((CustomQueryConfig.Snapshot) data);
                        else result.completeExceptionally(new IllegalStateException("Invalid custom query preview response"));
                    });
                } catch (Error e) {
                    result.completeExceptionally(e);
                    throw e;
                } catch (Exception e) {
                    result.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    /** Synchronous adapter for non-HTTP callers; the controller uses testAsync. */
    public CustomQueryConfig.Snapshot test(CustomQueryRequest request) throws Exception {
        try { return testAsync(request, "internal").get(); }
        catch (ExecutionException e) {
            if (e.getCause() instanceof Exception) throw (Exception) e.getCause();
            if (e.getCause() instanceof Error) throw (Error) e.getCause();
            throw new IllegalStateException("Custom query preview failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); throw e;
        }
    }
}

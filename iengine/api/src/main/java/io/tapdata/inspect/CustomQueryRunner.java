package io.tapdata.inspect;

import com.tapdata.tm.commons.inspect.CustomQueryConfig;
import com.tapdata.tm.commons.inspect.CustomQueryRequest;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

/** One bounded, cancellable invocation. Callbacks cross connector classloaders using JDK types only. */
public final class CustomQueryRunner {
    private static final ExecutorService WORKERS = new ThreadPoolExecutor(0, 16, 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), r -> { Thread t = new Thread(r, "inspect-custom-query"); t.setDaemon(true); return t; });
    private CustomQueryRunner() { }
    public static CustomQueryConfig.Snapshot execute(ConnectorNode node, CustomQueryRequest request,
            int maxRows, boolean preview, CustomQueryConfig.SideSummary summary) {
        return execute(node, request, maxRows, preview, summary, () -> { });
    }

    /** Cleanup belongs to the executing worker, including when a driver ignores cancellation. */
    public static CustomQueryConfig.Snapshot execute(ConnectorNode node, CustomQueryRequest request,
            int maxRows, boolean preview, CustomQueryConfig.SideSummary summary, Runnable cleanup) {
        AtomicBoolean submitted = new AtomicBoolean();
        try { return executeInternal(node, request, maxRows, preview, summary, cleanup, submitted); }
        finally { if (!submitted.get()) cleanup.run(); }
    }

    private static CustomQueryConfig.Snapshot executeInternal(ConnectorNode node, CustomQueryRequest request,
            int maxRows, boolean preview, CustomQueryConfig.SideSummary summary, Runnable cleanup, AtomicBoolean submitted) {
        summary.setStatus("error");
        request.validate();
        String connectorId = node.getConnectorContext().getSpecification().getId().toLowerCase(Locale.ROOT);
        if (connectorId.contains("mongo") != "mongoPipeline".equals(request.getKind()))
            throw new IllegalArgumentException("Query protocol does not match this connection's execution capability");
        if (maxRows < 1 || maxRows > 10000) throw new IllegalArgumentException("Invalid maximum rows");
        if (node.getConnectorFunctions().getExecuteCommandFunction() == null) throw new IllegalArgumentException("Connection does not support custom query execution");
        List<Map<String, Object>> rows = new ArrayList<>();
        List<CustomQueryConfig.Column> columns = new ArrayList<>();
        AtomicReference<Runnable> cancel = new AtomicReference<>();
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        AtomicBoolean stopped = new AtomicBoolean();
        CustomQueryConfig.Snapshot snapshot = new CustomQueryConfig.Snapshot();
        summary.setStartedAt(System.currentTimeMillis()); summary.setStatus("running");
        Consumer<Runnable> registerCancel = action -> { cancel.updateAndGet(previous -> previous == null ? action : () -> { try { previous.run(); } finally { action.run(); } }); if (stopped.get()) action.run(); };
        Map<String, Object> params = new HashMap<>();
        // Fetch window only; maxRows is enforced separately on every returned row.
        params.put("sql", request.getQuery()); params.put("batchSize", preview ? 1 : Math.min(maxRows + 1, 1000));
        params.put("queryTimeoutSeconds", request.getTimeoutSeconds());
        params.put("inspectRegisterCancel", registerCancel);
        params.put("inspectAllowEmptyPipeline", true);
        params.put("inspectColumns", (Consumer<List<Map<String, String>>>) schema -> {
            snapshot.setDeclaredSchema(true);
            if (!columns.isEmpty()) throw new IllegalArgumentException("Multiple result sets are not supported");
            for (Map<String, String> field : schema) {
                CustomQueryConfig.Column column = new CustomQueryConfig.Column();
                column.setName(field.get("name")); column.setType(field.get("type")); columns.add(column);
            }
        });
        String command = "executeQuery";
        if ("mongoPipeline".equals(request.getKind())) {
            command = "aggregate"; params.put("collection", request.getCollection()); params.put("pipeline", request.pipeline());
        }
        TapExecuteCommand execute = TapExecuteCommand.create().command(command).params(params).batchSize((Integer) params.get("batchSize"));
        AtomicReference<Thread> executingThread = new AtomicReference<>();
        Future<?> future = null;
        try {
            future = WORKERS.submit(() -> {
                synchronized (executingThread) { executingThread.set(Thread.currentThread()); }
                try {
                    PDKInvocationMonitor.invoke(node, PDKMethod.EXECUTE_COMMAND, () -> node.getConnectorFunctions().getExecuteCommandFunction().execute(node.getConnectorContext(), execute, result -> {
                        if (failure.get() != null) throw failure.get();
                        if (preview && snapshot.isHasMore()) throw new PreviewComplete();
                        if (stopped.get()) throw new CancellationException("Query cancelled");
                        if (result == null) return;
                        if (result.getError() != null) {
                            failure.set(new IllegalStateException("Custom query execution failed", result.getError())); throw failure.get();
                        }
                        Object data = result.getResult();
                        // Existing Paimon COUNT returns a scalar; JDBC update counts are rejected by the opt-in path.
                        if (data instanceof Number && connectorId.contains("paimon")) data = Collections.singletonList(Collections.singletonMap("value", data));
                        if (!(data instanceof List)) {
                            failure.set(new IllegalArgumentException("Query did not return a row set")); throw failure.get();
                        }
                        for (Object row : (List<?>) data) {
                            if (!(row instanceof Map)) { failure.set(new IllegalArgumentException("Query returned a non-row result")); throw failure.get(); }
                            if (rows.size() == maxRows) {
                                if (preview) { snapshot.setHasMore(true); throw new PreviewComplete(); }
                                failure.set(new IllegalArgumentException("Custom query exceeds maximum " + maxRows + " rows")); throw failure.get();
                            }
                            rows.add(new LinkedHashMap<>((Map<String, Object>) row));
                            summary.setRows(rows.size());
                        }
                    }), "CustomQueryRunner");
                    if (failure.get() != null) throw failure.get();
                } catch (Throwable e) {
                    if (e instanceof Error) throw (Error) e;
                    if (preview && snapshot.isHasMore() && failure.get() == null) return;
                    throw new CompletionException(failure.get() == null ? e : failure.get());
                } finally {
                    synchronized (executingThread) { executingThread.set(null); }
                    // Cancellation may have interrupted this thread. Cleanup must still be able to perform I/O.
                    Thread.interrupted();
                    cleanup.run();
                }
            });
            submitted.set(true);
            future.get(request.getTimeoutSeconds(), TimeUnit.SECONDS);
            // Document stores and older connectors have no declared schema: infer union from observed rows.
            if (columns.isEmpty()) {
                Map<String, String> types = new LinkedHashMap<>();
                for (Map<String, Object> row : rows) for (Map.Entry<String, Object> field : row.entrySet())
                    if (!types.containsKey(field.getKey()) || "null".equals(types.get(field.getKey()))) types.put(field.getKey(), field.getValue() == null ? "null" : field.getValue().getClass().getSimpleName());
                for (Map.Entry<String, String> field : types.entrySet()) { CustomQueryConfig.Column c = new CustomQueryConfig.Column(); c.setName(field.getKey()); c.setType(field.getValue()); columns.add(c); }
            }
            if (columns.isEmpty() && rows.isEmpty() && !connectorId.contains("mongo"))
                throw new IllegalStateException("Empty query result did not include column metadata; this connector cannot prove the output structure");
            snapshot.setRowCount(summary.getRows()); snapshot.setColumns(columns); snapshot.setRows(rows); snapshot.setFingerprint(request.fingerprint());
            summary.setStatus("success"); return snapshot;
        } catch (InterruptedException e) {
            summary.setStatus("cancelled"); Thread.currentThread().interrupt(); throw new CancellationException("Custom query cancelled");
        } catch (TimeoutException e) {
            summary.setStatus("timeout"); throw new IllegalStateException("Custom query timed out after " + request.getTimeoutSeconds() + " seconds", e);
        } catch (ExecutionException e) {
            summary.setStatus("error"); Throwable cause = e.getCause(); while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
            if (cause instanceof Error) throw (Error) cause;
            if (cause instanceof RuntimeException) throw (RuntimeException) cause;
            throw new IllegalStateException("Custom query failed", cause);
        } catch (RejectedExecutionException e) {
            summary.setStatus("error");
            // Preserve this type: formal jobs retry submission within their admission deadline.
            throw new RejectedExecutionException("Custom query capacity is exhausted; retry later", e);
        } catch (RuntimeException e) {
            if ("running".equals(summary.getStatus())) summary.setStatus("error");
            throw e;
        } finally {
            stopped.set(true);
            if (future != null && !future.isDone()) { Runnable action = cancel.get(); if (action != null) try { action.run(); } catch (Exception ignored) { } synchronized (executingThread) { Thread thread = executingThread.get(); if (thread != null) thread.interrupt(); } }
            summary.setEndedAt(System.currentTimeMillis()); summary.setDurationMillis(summary.getEndedAt() - summary.getStartedAt());
        }
    }
    private static final class PreviewComplete extends RuntimeException { }
}

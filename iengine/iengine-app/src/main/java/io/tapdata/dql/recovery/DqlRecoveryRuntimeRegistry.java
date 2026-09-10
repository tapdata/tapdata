package io.tapdata.dql.recovery;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keeps the live Engine source boundaries that belong to running tasks.
 *
 * <p>The WebSocket handler is created reflectively and the Jet source node is
 * created by the running task, so the coordinator needs a small runtime
 * bridge between those two lifecycles.  Only source boundaries are registered;
 * target and processor nodes are intentionally not exposed as recovery entry
 * points.</p>
 */
public final class DqlRecoveryRuntimeRegistry {
    private static final DqlRecoveryRuntimeRegistry GLOBAL = new DqlRecoveryRuntimeRegistry();

    private final ConcurrentMap<String, RuntimeTask> tasks = new ConcurrentHashMap<>();

    public static DqlRecoveryRuntimeRegistry global() {
        return GLOBAL;
    }

    public void register(String taskId,
                         Long taskVersion,
                         DAG dag,
                         String sourceNodeId,
                         DqlReplaySourceNode sourceBoundary) {
        requireText(taskId, "taskId");
        requireText(sourceNodeId, "sourceNodeId");
        Objects.requireNonNull(taskVersion, "taskVersion must not be null");
        Objects.requireNonNull(dag, "task DAG must not be null");
        Objects.requireNonNull(sourceBoundary, "source boundary must not be null");

        tasks.compute(taskId, (ignored, existing) -> {
            RuntimeTask runtime = existing;
            if (runtime == null || !Objects.equals(runtime.taskVersion(), taskVersion)) {
                runtime = new RuntimeTask(taskVersion, dag);
            }
            runtime.sourceBoundaries().put(sourceNodeId, sourceBoundary);
            return runtime;
        });
    }

    public void unregister(String taskId,
                           Long taskVersion,
                           String sourceNodeId,
                           DqlReplaySourceNode sourceBoundary) {
        if (isBlank(taskId) || isBlank(sourceNodeId) || sourceBoundary == null) {
            return;
        }
        tasks.computeIfPresent(taskId, (ignored, runtime) -> {
            if (!Objects.equals(runtime.taskVersion(), taskVersion)) {
                return runtime;
            }
            runtime.sourceBoundaries().remove(sourceNodeId, sourceBoundary);
            return runtime.sourceBoundaries().isEmpty() ? null : runtime;
        });
    }

    /**
     * Resolves a snapshot of the current task's source map.  Version matching
     * is mandatory so a stale source from a previous task generation cannot
     * receive a recovery event.
     */
    public DqlSourceBoundaryInjector openSourceBoundary(DqlRecoveryMessageDto command) {
        Objects.requireNonNull(command, "recovery command must not be null");
        RuntimeTask runtime = tasks.get(command.getTaskId());
        if (runtime == null) {
            throw new IllegalStateException("DLQ recovery source boundary is unavailable for task "
                    + command.getTaskId());
        }
        if (!Objects.equals(runtime.taskVersion(), command.getTaskVersion())) {
            throw new IllegalStateException("DLQ recovery source boundary task version is unavailable for task "
                    + command.getTaskId());
        }
        return new DqlSourceBoundaryInjector(runtime.dag(), runtime.sourceBoundaries());
    }

    public boolean contains(String taskId, Long taskVersion) {
        RuntimeTask runtime = tasks.get(taskId);
        return runtime != null && Objects.equals(runtime.taskVersion(), taskVersion)
                && !runtime.sourceBoundaries().isEmpty();
    }

    private static void requireText(String value, String field) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private record RuntimeTask(Long taskVersion,
                               DAG dag,
                               ConcurrentMap<String, DqlReplaySourceNode> sourceBoundaries) {
        private RuntimeTask(Long taskVersion, DAG dag) {
            this(taskVersion, dag, new ConcurrentHashMap<>());
        }

        @Override
        public ConcurrentMap<String, DqlReplaySourceNode> sourceBoundaries() {
            return sourceBoundaries;
        }
    }
}

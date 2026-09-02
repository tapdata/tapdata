package io.tapdata.dql.recovery;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlRecoveryNodeState;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Builds and owns the stopped-task temporary DAG used by production replay. */
public final class HazelcastDqlRecoveryBatchRuntimeFactory implements DqlRecoveryBatchRuntimeFactory {
    private final DqlRecoveryTaskLifecycle taskLifecycle;
    private final HazelcastTaskService taskService;
    private final HazelcastInstance hazelcastInstance;

    public HazelcastDqlRecoveryBatchRuntimeFactory(DqlRecoveryTaskLifecycle taskLifecycle,
                                                   HazelcastTaskService taskService,
                                                   HazelcastInstance hazelcastInstance) {
        this.taskLifecycle = Objects.requireNonNull(taskLifecycle, "taskLifecycle must not be null");
        this.taskService = Objects.requireNonNull(taskService, "taskService must not be null");
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance, "hazelcastInstance must not be null");
    }

    @Override
    public DqlRecoveryBatchRuntime open(DqlRecoveryMessageDto command, List<DqlRecoveryEvent> events) {
        Objects.requireNonNull(command, "recovery command must not be null");
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("DLQ recovery events must not be empty");
        }
        DqlRecoveryTaskSnapshot taskSnapshot = null;
        DqlRecoveryDagPlanner.Plan plan = null;
        HazelcastDqlRecoveryReplaySourceNode source = null;
        TaskClient<TaskDto> taskClient = null;
        try {
            taskSnapshot = taskLifecycle.stop(command);
            DqlRecoveryEvent first = requireEvent(events.get(0));
            validateSamePath(events, first);
            plan = DqlRecoveryDagPlanner.plan(taskSnapshot.task().getDag(),
                    first.failedNodeId(), first.targetNodeId());
            String queueName = "dql-recovery-" + command.getBatchId() + "-" + UUID.randomUUID();
            source = new HazelcastDqlRecoveryReplaySourceNode(hazelcastInstance, queueName);
            taskClient = taskService.startDqlRecoveryTask(taskSnapshot.task(), plan, queueName);
            if (taskClient == null) {
                throw new IllegalStateException("DLQ recovery temporary task client is unavailable");
            }
            return new Runtime(taskLifecycle, taskSnapshot, plan, source, taskClient, hazelcastInstance);
        } catch (Throwable exception) {
            if (exception instanceof DqlRecoveryTaskStopException stopException
                    && stopException.snapshot() != null) {
                // stop() may have captured the formal task state and then
                // failed before returning it.  Preserve that state so the
                // same compensation path can restore the formal task.
                taskSnapshot = stopException.snapshot();
            }
            boolean temporaryJobStopped = taskClient == null;
            if (taskClient != null) {
                try {
                    temporaryJobStopped = taskClient.stop();
                } catch (Throwable stopFailure) {
                    exception.addSuppressed(stopFailure);
                }
            }
            if (source != null) {
                try {
                    source.close();
                } catch (Throwable sourceFailure) {
                    exception.addSuppressed(sourceFailure);
                }
            }
            // Destroying the input queue can make a still-starting replay job
            // fail and enter a terminal state. Re-check once after queue
            // cleanup before deciding whether the formal task can be restored.
            if (taskClient != null && !temporaryJobStopped) {
                try {
                    temporaryJobStopped = taskClient.stop();
                } catch (Throwable stopFailure) {
                    exception.addSuppressed(stopFailure);
                }
            }
            if (plan != null) {
                try {
                    plan.restore();
                } catch (Throwable restoreFailure) {
                    exception.addSuppressed(restoreFailure);
                }
            }
            if (temporaryJobStopped && taskSnapshot != null) {
                try {
                    taskLifecycle.restore(taskSnapshot);
                } catch (Throwable restoreFailure) {
                    exception.addSuppressed(restoreFailure);
                }
            } else if (taskSnapshot != null) {
                exception.addSuppressed(new IllegalStateException(
                        "formal task remains stopped because the DQL recovery temporary job was not confirmed terminal"));
            }
            if (exception instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (exception instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("DLQ recovery runtime initialization failed", exception);
        }
    }

    private static DqlRecoveryEvent requireEvent(DqlRecoveryEvent event) {
        if (event == null || event.payload() == null || isBlank(event.failedNodeId())) {
            throw new IllegalArgumentException("DLQ event is missing failed node metadata");
        }
        return event;
    }

    private static void validateSamePath(List<DqlRecoveryEvent> events, DqlRecoveryEvent first) {
        for (DqlRecoveryEvent event : events) {
            requireEvent(event);
            if (!Objects.equals(first.failedNodeId(), event.failedNodeId())
                    || !Objects.equals(first.targetNodeId(), event.targetNodeId())) {
                throw new IllegalArgumentException(
                        "DLQ recovery batch contains events with different failed-node paths");
            }
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static final class Runtime implements DqlRecoveryBatchRuntime {
        private final DqlRecoveryTaskLifecycle taskLifecycle;
        private final DqlRecoveryTaskSnapshot taskSnapshot;
        private final DqlRecoveryDagPlanner.Plan plan;
        private final HazelcastDqlRecoveryReplaySourceNode source;
        private final TaskClient<TaskDto> taskClient;
        private final HazelcastInstance hazelcastInstance;
        private volatile boolean closed;
        private volatile List<DqlRecoveryNodeState> nodeStates;

        private Runtime(DqlRecoveryTaskLifecycle taskLifecycle,
                        DqlRecoveryTaskSnapshot taskSnapshot,
                        DqlRecoveryDagPlanner.Plan plan,
                        HazelcastDqlRecoveryReplaySourceNode source,
                        TaskClient<TaskDto> taskClient,
                        HazelcastInstance hazelcastInstance) {
            this.taskLifecycle = taskLifecycle;
            this.taskSnapshot = taskSnapshot;
            this.plan = plan;
            this.source = source;
            this.taskClient = taskClient;
            this.hazelcastInstance = hazelcastInstance;
            this.nodeStates = initialNodeStates(plan);
        }

        @Override
        public void enqueue(com.tapdata.entity.TapdataDqlRecoveryEvent event) {
            ensureOpen();
            source.enqueue(event);
        }

        @Override
        public DqlRecoveryBarrier barrier() {
            return new DqlRecoveryBarrierCoordinator(source, hazelcastInstance, this::jobFailure);
        }

        @Override
        public List<DqlRecoveryNodeState> nodeStates() {
            return nodeStates;
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            Throwable firstFailure = null;
            boolean temporaryJobStopped = false;
            try {
                temporaryJobStopped = taskClient.stop();
                if (!temporaryJobStopped) {
                    firstFailure = new IllegalStateException("DLQ recovery temporary job did not stop");
                }
            } catch (Throwable exception) {
                firstFailure = exception;
            }
            try {
                source.close();
            } catch (Throwable exception) {
                firstFailure = append(firstFailure, exception);
            }
            // A queue close may unblock a source processor that was still in
            // STARTING/COMPLETING. Re-check terminal state before restoring
            // the formal task.
            if (!temporaryJobStopped) {
                try {
                    temporaryJobStopped = taskClient.stop();
                } catch (Throwable exception) {
                    firstFailure = append(firstFailure, exception);
                }
            }
            try {
                taskClient.close();
            } catch (Throwable exception) {
                firstFailure = append(firstFailure, exception);
            }
            try {
                plan.restore();
                nodeStates = restoredNodeStates(plan);
            } catch (Throwable exception) {
                firstFailure = append(firstFailure, exception);
                nodeStates = failedRestoreNodeStates(plan, exception.getMessage());
            }
            if (temporaryJobStopped) {
                try {
                    taskLifecycle.restore(taskSnapshot);
                } catch (Throwable exception) {
                    firstFailure = append(firstFailure, exception);
                }
            } else {
                firstFailure = append(firstFailure, new IllegalStateException(
                        "formal task remains stopped because the DQL recovery temporary job was not confirmed terminal"));
            }
            closed = true;
            if (firstFailure != null) {
                if (firstFailure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (firstFailure instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException("DLQ recovery runtime cleanup failed", firstFailure);
            }
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("DLQ recovery temporary runtime is closed");
            }
        }

        private Throwable jobFailure() {
            try {
                return taskClient.getError();
            } catch (UnsupportedOperationException ignored) {
                return null;
            }
        }

        private static Throwable append(Throwable first, Throwable next) {
            if (first == null) {
                return next;
            }
            first.addSuppressed(next);
            return first;
        }

        private static List<DqlRecoveryNodeState> initialNodeStates(DqlRecoveryDagPlanner.Plan plan) {
            return plan.hiddenNodes().stream()
                    .map(node -> new DqlRecoveryNodeState(
                            node.nodeId(), node.nodeName(), node.disabledBefore(), node.disabledDuring(), false, null))
                    .toList();
        }

        private static List<DqlRecoveryNodeState> restoredNodeStates(DqlRecoveryDagPlanner.Plan plan) {
            return plan.hiddenNodes().stream()
                    .map(node -> new DqlRecoveryNodeState(
                            node.nodeId(), node.nodeName(), node.disabledBefore(), node.disabledDuring(), true, null))
                    .toList();
        }

        private static List<DqlRecoveryNodeState> failedRestoreNodeStates(DqlRecoveryDagPlanner.Plan plan,
                                                                           String message) {
            return plan.hiddenNodes().stream()
                    .map(node -> new DqlRecoveryNodeState(
                            node.nodeId(), node.nodeName(), node.disabledBefore(), node.disabledDuring(), false, message))
                    .toList();
        }
    }
}

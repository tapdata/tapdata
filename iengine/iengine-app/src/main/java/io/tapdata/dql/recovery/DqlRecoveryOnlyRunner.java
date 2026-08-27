package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a DQL batch against a paused task snapshot without starting the normal
 * task lifecycle or any connector source reader.
 */
public final class DqlRecoveryOnlyRunner implements AutoCloseable {
    @FunctionalInterface
    public interface Factory {
        /**
         * Returns a runner for a paused task, or {@code null} when the
         * already-running task should use the normal source boundary.
         */
        DqlRecoveryOnlyRunner open(com.tapdata.tm.dql.dto.DqlRecoveryMessageDto command);
    }

    private final TaskSnapshot taskSnapshot;
    private final DqlReplaySourceNode replaySourceNode;
    private final List<AutoCloseable> resources;
    private final AtomicBoolean closed = new AtomicBoolean();

    private DqlRecoveryOnlyRunner(TaskSnapshot taskSnapshot,
                                  DqlReplaySourceNode replaySourceNode,
                                  List<AutoCloseable> resources) {
        this.taskSnapshot = Objects.requireNonNull(taskSnapshot, "taskSnapshot must not be null");
        if (!taskSnapshot.isPaused()) {
            throw new IllegalArgumentException("recovery-only runner requires a paused task snapshot");
        }
        this.replaySourceNode = Objects.requireNonNull(replaySourceNode, "replaySourceNode must not be null");
        this.resources = new ArrayList<>(resources);
    }

    public static DqlRecoveryOnlyRunner open(TaskSnapshot taskSnapshot,
                                             DqlReplaySourceNode replaySourceNode,
                                             AutoCloseable... resources) {
        List<AutoCloseable> ownedResources = new ArrayList<>();
        if (resources != null) {
            for (AutoCloseable resource : resources) {
                if (resource != null) {
                    ownedResources.add(resource);
                }
            }
        }
        return new DqlRecoveryOnlyRunner(taskSnapshot, replaySourceNode, ownedResources);
    }

    public void replay(Iterable<TapdataDqlRecoveryEvent> events) {
        Objects.requireNonNull(events, "events must not be null");
        ensureOpen();
        for (TapdataDqlRecoveryEvent event : events) {
            if (event == null || !event.isDataEvent()) {
                throw new IllegalArgumentException("recovery-only runner accepts DATA events only");
            }
            replaySourceNode.enqueue(event);
        }
    }

    public void replay(TapdataDqlRecoveryEvent event) {
        replay(Collections.singletonList(event));
    }

    public TaskSnapshot taskSnapshot() {
        return taskSnapshot;
    }

    /** This runner never starts a normal connector source reader. */
    public boolean normalSourceStarted() {
        return false;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RuntimeException firstFailure = null;
        try {
            replaySourceNode.close();
        } catch (Exception exception) {
            firstFailure = new IllegalStateException("failed to close recovery replay source", exception);
        }
        List<AutoCloseable> reverse = new ArrayList<>(resources);
        Collections.reverse(reverse);
        for (AutoCloseable resource : reverse) {
            try {
                resource.close();
            } catch (Exception exception) {
                if (firstFailure == null) {
                    firstFailure = new IllegalStateException("failed to close recovery-only resource", exception);
                } else {
                    firstFailure.addSuppressed(exception);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("recovery-only runner is closed");
        }
    }

    public record TaskSnapshot(String taskId, Long taskVersion, String status) {
        public TaskSnapshot {
            if (taskId == null || taskId.isBlank()) {
                throw new IllegalArgumentException("taskId must not be blank");
            }
            if (taskVersion == null) {
                throw new IllegalArgumentException("taskVersion must not be null");
            }
        }

        public boolean isPaused() {
            return TaskDto.STATUS_STOP.equalsIgnoreCase(status)
                    || "paused".equalsIgnoreCase(status)
                    || "stopped".equalsIgnoreCase(status);
        }
    }
}

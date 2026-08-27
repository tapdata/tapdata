package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes one claimed recovery batch in message order.
 *
 * <p>The coordinator owns ordering and callback sequencing only. Source
 * lookup, source-boundary injection and target completion are intentionally
 * supplied as narrow ports so the same state machine can serve live and
 * paused-task runners.</p>
 */
public class DqlRecoveryCoordinatorImpl implements DqlRecoveryCoordinator {
    public static final long DEFAULT_BARRIER_TIMEOUT_MILLIS =
            DqlRuntimeConfig.DEFAULT_RECOVERY_EVENT_TIMEOUT_SECONDS * 1_000L;

    @FunctionalInterface
    public interface SourceBoundaryFactory {
        /**
         * Resolves the source boundary for a live task. The returned boundary
         * is owned by the running task and must not be closed by the
         * coordinator. Returning {@code null} preserves the legacy sink port.
         */
        DqlReplaySourceNode open(DqlRecoveryMessageDto command);
    }

    @FunctionalInterface
    public interface BarrierFactory {
        /** Creates the barrier for this batch and resolved source boundary. */
        DqlRecoveryBarrier open(DqlRecoveryMessageDto command, DqlReplaySourceNode sourceBoundary);
    }

    private final DqlRecoveryEventSource eventSource;
    private final DqlRecoveryEventSink eventSink;
    private final DqlRecoveryBarrier barrier;
    private final DqlRecoveryReportSender reportSender;
    private final DqlRecoveryExecutionPolicy executionPolicy;
    private final long barrierTimeoutMillis;
    private final Executor executor;
    private final DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory;
    private final SourceBoundaryFactory sourceBoundaryFactory;
    private final BarrierFactory barrierFactory;
    private final DqlRuntimeConfig runtimeConfig;
    private final ConcurrentMap<String, AtomicBoolean> activeBatches = new ConcurrentHashMap<>();
    private final Object idleMonitor = new Object();

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      Executor executor,
                                      DqlRuntimeConfig runtimeConfig) {
        this(eventSource, eventSink, barrier, reportSender,
                DqlRecoveryExecutionPolicy.from(runtimeConfig),
                barrierTimeoutMillis(runtimeConfig), executor, command -> null, command -> null,
                (command, sourceBoundary) -> barrier, effectiveConfig(runtimeConfig));
    }

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor) {
        this(eventSource, eventSink, barrier, reportSender, executionPolicy,
                barrierTimeoutMillis, executor, command -> null, command -> null,
                (command, sourceBoundary) -> barrier, DqlRuntimeConfig.defaults());
    }

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor,
                                      DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory) {
        this(eventSource, eventSink, barrier, reportSender, executionPolicy,
                barrierTimeoutMillis, executor, recoveryOnlyRunnerFactory, command -> null,
                (command, sourceBoundary) -> barrier, DqlRuntimeConfig.defaults());
    }

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor,
                                      DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory,
                                      SourceBoundaryFactory sourceBoundaryFactory) {
        this(eventSource, eventSink, barrier, reportSender, executionPolicy,
                barrierTimeoutMillis, executor, recoveryOnlyRunnerFactory, sourceBoundaryFactory,
                (command, sourceBoundary) -> barrier, DqlRuntimeConfig.defaults());
    }

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor,
                                      DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory,
                                      SourceBoundaryFactory sourceBoundaryFactory,
                                      BarrierFactory barrierFactory) {
        this(eventSource, eventSink, barrier, reportSender, executionPolicy, barrierTimeoutMillis, executor,
                recoveryOnlyRunnerFactory, sourceBoundaryFactory, barrierFactory, DqlRuntimeConfig.defaults());
    }

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor,
                                      DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory,
                                      SourceBoundaryFactory sourceBoundaryFactory,
                                      BarrierFactory barrierFactory,
                                      DqlRuntimeConfig runtimeConfig) {
        this.eventSource = Objects.requireNonNull(eventSource, "eventSource must not be null");
        this.eventSink = Objects.requireNonNull(eventSink, "eventSink must not be null");
        this.barrier = Objects.requireNonNull(barrier, "barrier must not be null");
        this.reportSender = Objects.requireNonNull(reportSender, "reportSender must not be null");
        this.executionPolicy = Objects.requireNonNull(executionPolicy, "executionPolicy must not be null");
        if (barrierTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("barrierTimeoutMillis must be greater than zero");
        }
        this.barrierTimeoutMillis = barrierTimeoutMillis;
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.recoveryOnlyRunnerFactory = Objects.requireNonNull(
                recoveryOnlyRunnerFactory, "recoveryOnlyRunnerFactory must not be null");
        this.sourceBoundaryFactory = Objects.requireNonNull(
                sourceBoundaryFactory, "sourceBoundaryFactory must not be null");
        this.barrierFactory = Objects.requireNonNull(barrierFactory, "barrierFactory must not be null");
        this.runtimeConfig = Objects.requireNonNull(runtimeConfig, "runtimeConfig must not be null");
    }

    @Override
    public void start(DqlRecoveryMessageDto command) {
        validateCommand(command);
        String batchId = command.getBatchId();
        List<String> orderedEventIds = List.copyOf(command.getOrderedEventIds());
        AtomicBoolean terminal = new AtomicBoolean(false);
        if (activeBatches.putIfAbsent(batchId, terminal) != null) {
            throw new IllegalStateException("recovery batch is already running: " + batchId);
        }
        try {
            executor.execute(() -> executeBatch(command, orderedEventIds, terminal));
        } catch (RuntimeException exception) {
            activeBatches.remove(batchId, terminal);
            signalIdle();
            throw exception;
        }
    }

    /** Test and lifecycle hook used by later runner integrations. */
    public boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit must not be null");
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (idleMonitor) {
            while (!activeBatches.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(idleMonitor, remaining);
            }
            return true;
        }
    }

    private void executeBatch(DqlRecoveryMessageDto command,
                              List<String> orderedEventIds,
                              AtomicBoolean terminal) {
        DqlRecoveryOnlyRunner recoveryOnlyRunner = null;
        DqlRecoveryFailureCompensator compensator = new DqlRecoveryFailureCompensator(
                failure -> failBatchOnce(command, terminal, failure));
        try {
            recoveryOnlyRunner = recoveryOnlyRunnerFactory.open(command);
            DqlRecoveryEventSink sink;
            DqlReplaySourceNode sourceBoundary = null;
            if (recoveryOnlyRunner != null) {
                compensator.addCleanup(recoveryOnlyRunner::close);
                sink = recoveryOnlyRunner::replay;
                sourceBoundary = recoveryOnlyRunner;
            } else {
                sourceBoundary = sourceBoundaryFactory.open(command);
                sink = sourceBoundary == null ? eventSink : sourceBoundary::enqueue;
                if (sourceBoundary != null) {
                    // Register restoration before preparation so a partially
                    // entered source gate is also given a recovery attempt.
                    compensator.addCleanup(sourceBoundary::restoreAfterRecovery);
                    sourceBoundary.prepareForRecovery(barrierTimeoutMillis);
                }
            }
            DqlRecoveryBarrier batchBarrier = barrierFactory.open(command, sourceBoundary);
            if (batchBarrier == null) {
                batchBarrier = barrier;
            }
            for (String eventId : orderedEventIds) {
                if (terminal.get()) {
                    return;
                }
                batchBarrier.register(eventId);
                EventExecutionResult result = executeEvent(command, eventId, sink, batchBarrier);
                if (result.successful()) {
                    continue;
                }
                if (!executionPolicy.continueAfterFailure()) {
                    compensator.compensate(new IllegalStateException(result.message()));
                    return;
                }
            }
            Throwable cleanupFailure = compensator.cleanup();
            if (cleanupFailure != null) {
                compensator.compensate(cleanupFailure);
                return;
            }
            finishBatchOnce(command, terminal);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            compensator.compensate(exception);
        } catch (RuntimeException exception) {
            compensator.compensate(exception);
        } finally {
            activeBatches.remove(command.getBatchId(), terminal);
            signalIdle();
        }
    }

    private EventExecutionResult executeEvent(DqlRecoveryMessageDto command,
                                              String eventId,
                                              DqlRecoveryEventSink sink,
                                              DqlRecoveryBarrier batchBarrier) {
        String attemptId = UUID.randomUUID().toString();
        long startedAt = System.currentTimeMillis();
        EventExecutionResult result;
        try {
            reportSender.reportEventStarted(command, eventId, attemptId, startedAt);
            DqlPayloadSnapshot snapshot = eventSource.load(eventId);
            if (snapshot == null) {
                throw new IllegalStateException("DQL event payload was not found: " + eventId);
            }
            TapdataDqlRecoveryEvent recoveryEvent = TapdataDqlRecoveryEvent.createData(
                    command.getBatchId(),
                    eventId,
                    attemptId,
                    command.getOperatorId(),
                    command.getTaskVersion(),
                    snapshot,
                    runtimeConfig
            );
            sink.enqueue(recoveryEvent);
            result = barrierResult(eventId, batchBarrier.await(eventId, barrierTimeoutMillis));
        } catch (InterruptedException exception) {
            batchBarrier.cancel(eventId);
            Thread.currentThread().interrupt();
            result = EventExecutionResult.failureResult("recovery barrier interrupted for event " + eventId,
                    "TIMEOUT");
        } catch (RuntimeException exception) {
            batchBarrier.cancel(eventId);
            result = EventExecutionResult.failureResult(message(exception), "FAILED");
        }

        try {
            reportSender.reportEventResult(
                    command,
                    eventId,
                    attemptId,
                    result.result(),
                    result.message(),
                    startedAt,
                    System.currentTimeMillis()
            );
        } catch (RuntimeException exception) {
            batchBarrier.cancel(eventId);
            throw exception;
        }
        return result;
    }

    private EventExecutionResult barrierResult(String eventId, DqlRecoveryBarrier.Outcome outcome) {
        if (outcome == DqlRecoveryBarrier.Outcome.SUCCESS) {
            return EventExecutionResult.successResult();
        }
        if (outcome == DqlRecoveryBarrier.Outcome.TIMEOUT) {
            return EventExecutionResult.failureResult("recovery barrier timed out for event " + eventId, "TIMEOUT");
        }
        return EventExecutionResult.failureResult("recovery barrier failed for event " + eventId, "FAILED");
    }

    private void finishBatchOnce(DqlRecoveryMessageDto command, AtomicBoolean terminal) {
        if (terminal.compareAndSet(false, true)) {
            try {
                reportSender.reportBatchFinished(command, null, System.currentTimeMillis());
            } catch (RuntimeException exception) {
                terminal.compareAndSet(true, false);
                throw exception;
            }
        }
    }

    private void failBatchOnce(DqlRecoveryMessageDto command, AtomicBoolean terminal, String message) {
        if (terminal.compareAndSet(false, true)) {
            reportSender.reportBatchFailed(command, message, System.currentTimeMillis());
        }
    }

    private void validateCommand(DqlRecoveryMessageDto command) {
        if (command == null) {
            throw new IllegalArgumentException("recovery command must not be null");
        }
        if (StringUtils.isBlank(command.getBatchId())) {
            throw new IllegalArgumentException("recovery command batchId must not be blank");
        }
        if (command.getOrderedEventIds() == null || command.getOrderedEventIds().isEmpty()) {
            throw new IllegalArgumentException("recovery command orderedEventIds must not be empty");
        }
    }

    private String message(Throwable exception) {
        return StringUtils.defaultIfBlank(exception.getMessage(), exception.getClass().getSimpleName());
    }

    private void signalIdle() {
        synchronized (idleMonitor) {
            idleMonitor.notifyAll();
        }
    }

    private static DqlRuntimeConfig effectiveConfig(DqlRuntimeConfig config) {
        return config == null ? DqlRuntimeConfig.defaults() : config;
    }

    private static long barrierTimeoutMillis(DqlRuntimeConfig config) {
        return effectiveConfig(config).getRecoveryEventTimeoutSeconds() * 1_000L;
    }

    private record EventExecutionResult(boolean successful, String result, String message) {
        private static EventExecutionResult successResult() {
            return new EventExecutionResult(true, "SUCCESS", null);
        }

        private static EventExecutionResult failureResult(String message, String result) {
            return new EventExecutionResult(false, result, message);
        }
    }
}

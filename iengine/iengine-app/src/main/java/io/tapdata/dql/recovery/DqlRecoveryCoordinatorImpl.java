package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
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
    public static final long DEFAULT_BARRIER_TIMEOUT_MILLIS = 30_000L;

    private final DqlRecoveryEventSource eventSource;
    private final DqlRecoveryEventSink eventSink;
    private final DqlRecoveryBarrier barrier;
    private final DqlRecoveryReportSender reportSender;
    private final DqlRecoveryExecutionPolicy executionPolicy;
    private final long barrierTimeoutMillis;
    private final Executor executor;
    private final DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory;
    private final ConcurrentMap<String, AtomicBoolean> activeBatches = new ConcurrentHashMap<>();
    private final Object idleMonitor = new Object();

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor) {
        this(eventSource, eventSink, barrier, reportSender, executionPolicy,
                barrierTimeoutMillis, executor, command -> null);
    }

    public DqlRecoveryCoordinatorImpl(DqlRecoveryEventSource eventSource,
                                      DqlRecoveryEventSink eventSink,
                                      DqlRecoveryBarrier barrier,
                                      DqlRecoveryReportSender reportSender,
                                      DqlRecoveryExecutionPolicy executionPolicy,
                                      long barrierTimeoutMillis,
                                      Executor executor,
                                      DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory) {
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
        try {
            recoveryOnlyRunner = recoveryOnlyRunnerFactory.open(command);
            DqlRecoveryEventSink sink = recoveryOnlyRunner == null
                    ? eventSink
                    : recoveryOnlyRunner::replay;
            for (String eventId : orderedEventIds) {
                if (terminal.get()) {
                    return;
                }
                EventExecutionResult result = executeEvent(command, eventId, sink);
                if (result.successful()) {
                    continue;
                }
                if (!executionPolicy.continueAfterFailure()) {
                    failBatchOnce(command, terminal, result.message());
                    return;
                }
            }
            finishBatchOnce(command, terminal);
        } catch (RuntimeException exception) {
            failBatchOnce(command, terminal, message(exception));
        } finally {
            if (recoveryOnlyRunner != null) {
                try {
                    recoveryOnlyRunner.close();
                } catch (RuntimeException exception) {
                    failBatchOnce(command, terminal, message(exception));
                }
            }
            activeBatches.remove(command.getBatchId(), terminal);
            signalIdle();
        }
    }

    private EventExecutionResult executeEvent(DqlRecoveryMessageDto command,
                                              String eventId,
                                              DqlRecoveryEventSink sink) {
        String attemptId = UUID.randomUUID().toString();
        long startedAt = System.currentTimeMillis();
        reportSender.reportEventStarted(command, eventId, attemptId, startedAt);

        EventExecutionResult result;
        try {
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
                    snapshot
            );
            sink.enqueue(recoveryEvent);
            result = barrierResult(eventId, barrier.await(eventId, barrierTimeoutMillis));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            result = EventExecutionResult.failureResult("recovery barrier interrupted for event " + eventId,
                    "TIMEOUT");
        } catch (RuntimeException exception) {
            result = EventExecutionResult.failureResult(message(exception), "FAILED");
        }

        reportSender.reportEventResult(
                command,
                eventId,
                attemptId,
                result.result(),
                result.message(),
                startedAt,
                System.currentTimeMillis()
        );
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
            reportSender.reportBatchFinished(command, null, System.currentTimeMillis());
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

    private String message(RuntimeException exception) {
        return StringUtils.defaultIfBlank(exception.getMessage(), exception.getClass().getSimpleName());
    }

    private void signalIdle() {
        synchronized (idleMonitor) {
            idleMonitor.notifyAll();
        }
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

package com.tapdata.tm.taskrebalance.service;

import com.tapdata.tm.taskrebalance.repository.TaskRebalanceJobRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TaskRebalanceJobServiceTest {

    private final TaskRebalanceJobService jobService =
            new TaskRebalanceJobService(mock(TaskRebalanceJobRepository.class));

    @Test
    @DisplayName("default state has no rebalance flags")
    void defaultFlagsAreFalse() {
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("runWithoutRebalanceCheck only bypasses the check flag")
    void bypassFlagOnly() {
        jobService.runWithoutRebalanceCheck(() -> {
            assertTrue(jobService.isCheckBypassed());
            assertFalse(jobService.isRebalanceOperation());
        });
        assertFalse(jobService.isCheckBypassed());
    }

    @Test
    @DisplayName("runAsRebalanceOperation sets both flags and restores them")
    void rebalanceOperationFlags() {
        jobService.runAsRebalanceOperation(() -> {
            assertTrue(jobService.isCheckBypassed());
            assertTrue(jobService.isRebalanceOperation());
        });
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("nested runAsRebalanceOperation restores outer state correctly")
    void nestedRestoresOuterState() {
        jobService.runWithoutRebalanceCheck(() ->
                jobService.runAsRebalanceOperation(() -> {
                    assertTrue(jobService.isCheckBypassed());
                    assertTrue(jobService.isRebalanceOperation());
                })
        );
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("flags are properly restored even when the runnable throws")
    void flagsRestoredOnException() {
        try {
            jobService.runAsRebalanceOperation(() -> {
                throw new RuntimeException("boom");
            });
        } catch (RuntimeException ignored) {
        }
        assertFalse(jobService.isCheckBypassed());
        assertFalse(jobService.isRebalanceOperation());
    }

    @Test
    @DisplayName("flags are thread-local and do not leak across threads")
    void flagsAreThreadLocal() throws InterruptedException {
        final boolean[] otherThreadBypassed = {true};
        final boolean[] otherThreadRebalance = {true};
        jobService.runAsRebalanceOperation(() -> {
            Thread other = new Thread(() -> {
                otherThreadBypassed[0] = jobService.isCheckBypassed();
                otherThreadRebalance[0] = jobService.isRebalanceOperation();
            });
            other.start();
            try {
                other.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertFalse(otherThreadBypassed[0]);
        assertFalse(otherThreadRebalance[0]);
    }
}

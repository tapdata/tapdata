package com.tapdata.tm.group.dto;

import lombok.Data;

import java.util.Date;

/**
 * Git operation step record
 * Used to track individual steps during Git export operations
 */
@Data
public class GitOperationStep {
    /**
     * Step name (e.g., "Clone Repository", "Commit Changes", "Push to Remote")
     */
    private String stepName;

    /**
     * Step status: SUCCESS, FAILED, IN_PROGRESS
     */
    private StepStatus status;

    /**
     * Message (success message or error message)
     */
    private String message;

    /**
     * Timestamp when the step was executed
     */
    private Date timestamp;

    /**
     * Duration in milliseconds (optional)
     */
    private Long durationMs;

	private String stackTrace;

    public enum StepStatus {
        IN_PROGRESS,
        SUCCESS,
        FAILED
    }

    public GitOperationStep() {
        this.timestamp = new Date();
    }

    public GitOperationStep(String stepName, StepStatus status, String message) {
        this.stepName = stepName;
        this.status = status;
        this.message = message;
        this.timestamp = new Date();
    }
}


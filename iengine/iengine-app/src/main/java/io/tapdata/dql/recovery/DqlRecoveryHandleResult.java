package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import lombok.Getter;

/** Result returned by the Engine recovery message boundary. */
@Getter
public final class DqlRecoveryHandleResult {
    public enum Outcome {
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }

    private final Outcome outcome;
    private final DqlRecoveryMessageDto command;
    private final String message;

    private DqlRecoveryHandleResult(Outcome outcome, DqlRecoveryMessageDto command, String message) {
        this.outcome = outcome;
        this.command = command;
        this.message = message;
    }

    public static DqlRecoveryHandleResult accepted(DqlRecoveryMessageDto command) {
        return new DqlRecoveryHandleResult(Outcome.ACCEPTED, command, "Recovery batch accepted");
    }

    public static DqlRecoveryHandleResult duplicate(DqlRecoveryMessageDto command) {
        return new DqlRecoveryHandleResult(Outcome.DUPLICATE, command, "Recovery batch was already accepted");
    }

    public static DqlRecoveryHandleResult rejected(String message) {
        return new DqlRecoveryHandleResult(Outcome.REJECTED, null, message);
    }

    public String getBatchId() {
        return command == null ? null : command.getBatchId();
    }

    public boolean isStarted() {
        return outcome == Outcome.ACCEPTED;
    }
}

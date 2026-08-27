package io.tapdata.dql.recovery;

/**
 * Starts one already validated DQL recovery batch.
 *
 * <p>The message boundary owns validation and idempotency. The coordinator
 * owns execution and is implemented by the later recovery steps.</p>
 */
public interface DqlRecoveryCoordinator {
    void start(com.tapdata.tm.dql.dto.DqlRecoveryMessageDto command);
}

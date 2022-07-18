package io.tapdata.common;

/**
 * data flow 状态
 *
 * @since 3.0
 */
public enum DataFlowStatus {

	/**
	 * The job is currently running.
	 */
	RUNNING,

	/**
	 * The job is currently being paused.
	 */
	PAUSED,

	/**
	 * The job is currently being scheduled.
	 */
	SCHEDULED,

	/**
	 * The job is currently being STOPPING.
	 */
	STOPPING,

	/**
	 * The job is currently being force stopping.
	 */
	FORCE_STOPPING,

	/**
	 * The job is currently being error.
	 */
	ERROR,

	/**
	 * The job is paused, but cannot modify status in tapdata mongodb
	 */
	INTERNAL_PAUSED,
	;

	/**
	 * Returns {@code true} if this state is terminal - a job in this state
	 * will never have any other state and will never execute again. It's
	 * {@link #PAUSED} or {@link #ERROR}.
	 */
	public boolean isTerminal() {
		return this == PAUSED || this == ERROR;
	}
}

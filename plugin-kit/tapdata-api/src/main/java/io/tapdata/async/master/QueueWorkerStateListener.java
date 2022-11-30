package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface QueueWorkerStateListener {
	int STATE_NONE = 1;
	int STATE_IDLE = 5;
	int STATE_LONG_IDLE = 8;
	int STATE_RUNNING = 10;
	int STATE_STOPPED = 100;
	void stateChanged(String id, int fromState, int toState);
}

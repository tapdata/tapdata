package io.tapdata.async.master;

/**
 * @author aplomb
 */
public interface ParallelWorkerStateListener {
	int STATE_NONE = 1;
	int STATE_RUNNING = 10;
	int STATE_FINISHED = 50;
	int STATE_STOPPED = 100;
	void stateChanged(String id, int fromState, int toState);
}

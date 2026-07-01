package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2022-05-19 16:06
 **/
public class TapdataCompleteSnapshotEvent extends TapdataEvent {
	private static final long serialVersionUID = 5717006177280281597L;

	public boolean allCompleted = true;

	public TapdataCompleteSnapshotEvent() {
		this.syncStage = SyncStage.INITIAL_SYNC;
	}

	public void completing() {
		allCompleted = false;
	}

	public void completeAll() {
		allCompleted = true;
	}

	public boolean isAllCompleted() {
		return allCompleted;
	}

	@Override
	public boolean isConcurrentWrite() {
		return false;
	}
}

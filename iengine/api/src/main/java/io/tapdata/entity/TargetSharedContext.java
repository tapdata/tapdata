package io.tapdata.entity;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TargetSharedContext {

	private AtomicBoolean ddlCompleted = new AtomicBoolean(false);

	private AtomicBoolean timingExecutorExists = new AtomicBoolean(false);

	/**
	 * 最后一条insert写入的消息的时间
	 * 默认0：表示没有以insert模式写入的消息
	 */
	private AtomicLong lastInsertModeTs = new AtomicLong(0l);

	private AtomicBoolean upsert = new AtomicBoolean(true);

	public AtomicBoolean getTimingExecutorExists() {
		return timingExecutorExists;
	}

	public void setTimingExecutorExists(AtomicBoolean timingExecutorExists) {
		this.timingExecutorExists = timingExecutorExists;
	}

	public AtomicBoolean getDdlCompleted() {
		return ddlCompleted;
	}

	public void setDdlCompleted(AtomicBoolean ddlCompleted) {
		this.ddlCompleted = ddlCompleted;
	}

	public AtomicLong getLastInsertModeTs() {
		return lastInsertModeTs;
	}

	public AtomicBoolean getUpsert() {
		return upsert;
	}
}

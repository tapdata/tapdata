package io.tapdata.flow.engine.V2.task.preview;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-10-10 11:27
 **/
public class TaskPReviewStatsVO {
	private long parseTaskJsonTaken;
	private long beforeTaken;
	private long execTaskTaken;
	private long stopTaskTaken;
	private long afterTaken;
	private long allTaken;
	private long taskletTaken;
	private Map<String, TaskPreviewReadStatsVO> readStats = new HashMap<>();

	public long getParseTaskJsonTaken() {
		return parseTaskJsonTaken;
	}

	public void setParseTaskJsonTaken(long parseTaskJsonTaken) {
		this.parseTaskJsonTaken = parseTaskJsonTaken;
	}

	public long getBeforeTaken() {
		return beforeTaken;
	}

	public void setBeforeTaken(long beforeTaken) {
		this.beforeTaken = beforeTaken;
	}

	public long getExecTaskTaken() {
		return execTaskTaken;
	}

	public void setExecTaskTaken(long execTaskTaken) {
		this.execTaskTaken = execTaskTaken;
	}

	public long getAfterTaken() {
		return afterTaken;
	}

	public void setAfterTaken(long afterTaken) {
		this.afterTaken = afterTaken;
	}

	public long getAllTaken() {
		return allTaken;
	}

	public void setAllTaken(long allTaken) {
		this.allTaken = allTaken;
	}

	public long getTaskletTaken() {
		return taskletTaken;
	}

	public void setTaskletTaken(long taskletTaken) {
		this.taskletTaken = taskletTaken;
	}

	public long getStopTaskTaken() {
		return stopTaskTaken;
	}

	public void setStopTaskTaken(long stopTaskTaken) {
		this.stopTaskTaken = stopTaskTaken;
	}

	public Map<String, TaskPreviewReadStatsVO> getReadStats() {
		return readStats;
	}

	public void setReadStats(Map<String, TaskPreviewReadStatsVO> readStats) {
		this.readStats = readStats;
	}
}

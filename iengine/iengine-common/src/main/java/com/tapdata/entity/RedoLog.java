package com.tapdata.entity;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

/**
 * @author huangjq
 * @ClassName: RedoLog
 * @Description: 归档日志实体
 * @date 17-8-18 上午10:37
 * @since 1.0
 */
public class RedoLog implements Comparable<RedoLog> {
	/**
	 * archived log location
	 */
	private String name;

	/**
	 * first_change scn
	 */
	private long firstChangeScn;

	/**
	 * Archived log record stamp
	 */
	private long stamp;


	/**
	 * Redo log sequence number
	 */
	private long sequence;

	/**
	 * online redo log status
	 * CURRENT : using
	 * INACTIVE : unuse
	 */
	private String status;

	/**
	 *
	 */
	private Long thread;

	private Date firstTime;

	private long sizeInMB;

	public RedoLog() {
	}

	public static RedoLog archivedLog(ResultSet resultSet) throws SQLException {
		RedoLog redoLog = new RedoLog();
		redoLog.setName(resultSet.getString("NAME"));
		redoLog.setFirstChangeScn(resultSet.getLong("FIRST_CHANGE#"));
		redoLog.setSequence(resultSet.getLong("SEQUENCE#"));
		redoLog.setStatus(resultSet.getString("STATUS"));
		redoLog.setStatus(resultSet.getString("STATUS"));
		redoLog.setFirstTime(resultSet.getDate("FIRST_TIME"));
		redoLog.setThread(resultSet.getLong("THREAD#"));

		long blocks = resultSet.getLong("BLOCKS");
		long blockSize = resultSet.getLong("BLOCK_SIZE");

		redoLog.setSizeInMB(blocks * blockSize / 1024 / 1024);

		return redoLog;
	}

	public static RedoLog onlineLog(ResultSet resultSet, String version) throws SQLException {
		RedoLog redoLog = new RedoLog();
		redoLog.setName(resultSet.getString("NAME"));
		redoLog.setFirstChangeScn(resultSet.getLong("FIRST_CHANGE#"));
		redoLog.setSequence(resultSet.getLong("SEQUENCE#"));
		redoLog.setStatus(resultSet.getString("STATUS"));
		redoLog.setFirstTime(resultSet.getDate("FIRST_TIME"));

		redoLog.setThread(resultSet.getLong("THREAD#"));

		redoLog.setSizeInMB(resultSet.getLong("SIZEINMB"));

		return redoLog;
	}

	public RedoLog(String name, long firstChangeScn) {
		this.name = name;
		this.firstChangeScn = firstChangeScn;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getFirstChangeScn() {
		return firstChangeScn;
	}

	public void setFirstChangeScn(long firstChangeScn) {
		this.firstChangeScn = firstChangeScn;
	}

	public long getStamp() {
		return stamp;
	}

	public void setStamp(long stamp) {
		this.stamp = stamp;
	}

	public long getSequence() {
		return sequence;
	}

	public void setSequence(long sequence) {
		this.sequence = sequence;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getSizeInMB() {
		return sizeInMB;
	}

	public void setSizeInMB(long sizeInMB) {
		this.sizeInMB = sizeInMB;
	}

	public Date getFirstTime() {
		return firstTime;
	}

	public void setFirstTime(Date firstTime) {
		this.firstTime = firstTime;
	}

	public Long getThread() {
		return thread;
	}

	public void setThread(Long thread) {
		this.thread = thread;
	}

	@Override
	public String toString() {
		return "RedoLog{" +
				"name='" + name + '\'' +
				", firstChangeScn=" + firstChangeScn +
				", stamp=" + stamp +
				", sequence=" + sequence +
				", status='" + status + '\'' +
				", thread=" + thread +
				", firstTime=" + firstTime +
				", sizeInMB=" + sizeInMB +
				'}';
	}

	@Override
	public int compareTo(RedoLog o) {
		if (o == null) {
			return 1;
		}
		if (this.getFirstChangeScn() > o.getFirstChangeScn()) {
			return 1;
		} else if (this.getFirstChangeScn() < o.getFirstChangeScn()) {
			return -1;
		}
		return 0;
	}
}

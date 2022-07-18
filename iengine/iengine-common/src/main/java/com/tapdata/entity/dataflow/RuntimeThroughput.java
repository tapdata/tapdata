package com.tapdata.entity.dataflow;

import java.io.Serializable;

/**
 * @author jackin
 */
public class RuntimeThroughput implements Serializable {

	private static final long serialVersionUID = -4944611064967049958L;
	/**
	 * total rows
	 */
	private long rows;

	/**
	 * data size, unit KB
	 */

	private long dataSize;


	public RuntimeThroughput() {
	}

	public RuntimeThroughput(long rows, long dataSize) {
		this.rows = rows;
		this.dataSize = dataSize;
	}

	public RuntimeThroughput(RuntimeThroughput runtimeThroughput) {
		this.rows = runtimeThroughput.getRows();
		this.dataSize = runtimeThroughput.getDataSize();
	}

	public synchronized void incrementStats(RuntimeThroughput throughput) {
		this.rows += throughput.getRows();
		this.dataSize += throughput.getDataSize();
	}

	public long getRows() {
		return rows;
	}

	public synchronized void incrementRows(long rows) {
		this.rows += rows;
	}

	public synchronized void incrementDataSize(long dataSize) {
		this.dataSize += dataSize;
	}

	public void setRows(long rows) {
		this.rows = rows;
	}

	public long getDataSize() {
		return dataSize;
	}

	public void setDataSize(long dataSize) {
		this.dataSize = dataSize;
	}

	public void makeZero() {
		this.rows = 0L;
		this.dataSize = 0L;
	}

	@Override
	public String toString() {
		return "RuntimeThroughput{" +
				"rows=" + rows +
				", dataSize=" + dataSize +
				'}';
	}
}

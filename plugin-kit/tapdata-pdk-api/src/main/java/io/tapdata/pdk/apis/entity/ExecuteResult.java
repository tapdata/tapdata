package io.tapdata.pdk.apis.entity;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class ExecuteResult {
	private List<? extends Map<String, Object>> results;
	public ExecuteResult results(List<? extends Map<String, Object>> results) {
		this.results = results;
		return this;
	}
	private long insertedCount;

	public ExecuteResult insertedCount(long insertedCount) {
		this.insertedCount = insertedCount;
		return this;
	}

	private long removedCount;

	public ExecuteResult removedCount(long removedCount) {
		this.removedCount = removedCount;
		return this;
	}

	private long modifiedCount;

	public ExecuteResult modifiedCount(long modifiedCount) {
		this.modifiedCount = modifiedCount;
		return this;
	}

	private Throwable error;


	public ExecuteResult() {
	}

	public ExecuteResult(long insertedCount, long modifiedCount, long removedCount) {
		this(insertedCount, modifiedCount, removedCount, null);
	}

	public ExecuteResult(long insertedCount, long modifiedCount, long removedCount, Throwable throwable) {
		this.insertedCount = insertedCount;
		this.modifiedCount = modifiedCount;
		this.removedCount = removedCount;
		this.error = throwable;
	}

	public long getInsertedCount() {
		return insertedCount;
	}

	public void setInsertedCount(long insertedCount) {
		this.insertedCount = insertedCount;
	}

	public long getRemovedCount() {
		return removedCount;
	}

	public void setRemovedCount(long removedCount) {
		this.removedCount = removedCount;
	}

	public long getModifiedCount() {
		return modifiedCount;
	}

	public void setModifiedCount(long modifiedCount) {
		this.modifiedCount = modifiedCount;
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}

	public void incrementInserted(long value) {
		this.insertedCount = this.insertedCount + value;
	}

	public void incrementModified(long value) {
		this.modifiedCount = this.modifiedCount + value;
	}

	public void incrementRemove(long value) {
		this.removedCount = this.removedCount + value;
	}

	public List<? extends Map<String, Object>> getResults() {
		return results;
	}

	public void setResults(List<? extends Map<String, Object>> results) {
		this.results = results;
	}
}

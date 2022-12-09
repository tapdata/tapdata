package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.event.TapEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class ExecuteResult<T extends TapEvent> {
	private List<T> events;
	public ExecuteResult<T> events(List<T> events) {
		this.events = events;
		return this;
	}
	private long insertedCount;

	public ExecuteResult<T> insertedCount(long insertedCount) {
		this.insertedCount = insertedCount;
		return this;
	}

	private long removedCount;

	public ExecuteResult<T> removedCount(long removedCount) {
		this.removedCount = removedCount;
		return this;
	}

	private long modifiedCount;

	public ExecuteResult<T> modifiedCount(long modifiedCount) {
		this.modifiedCount = modifiedCount;
		return this;
	}

	private Map<T, Throwable> errorMap;

	public ExecuteResult<T> addError(T key, Throwable value) {
		if (errorMap == null) {
			errorMap = new HashMap<>();
		}
		errorMap.put(key, value);
		return this;
	}

	public ExecuteResult() {
	}

	public ExecuteResult(long insertedCount, long modifiedCount, long removedCount) {
		this(insertedCount, modifiedCount, removedCount, null);
	}

	public ExecuteResult(long insertedCount, long modifiedCount, long removedCount, Map<T, Throwable> errorMap) {
		this.insertedCount = insertedCount;
		this.modifiedCount = modifiedCount;
		this.removedCount = removedCount;
		this.errorMap = errorMap;
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

	public Map<T, Throwable> getErrorMap() {
		return errorMap;
	}

	public void setErrorMap(Map<T, Throwable> errorMap) {
		this.errorMap = errorMap;
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

	public void addErrors(Map<T, Throwable> map) {
		if (errorMap == null) {
			errorMap = new HashMap<>();
		}
		errorMap.putAll(map);
	}

	public List<T> getEvents() {
		return events;
	}

	public void setEvents(List<T> events) {
		this.events = events;
	}
}

package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SyncProgress implements Serializable, Comparable<SyncProgress> {
	private static final long serialVersionUID = 5599838762323297718L;
	public static final String OFFSET = "offset";
	public static final String STATUS = "status";
	public static final String TABLE_BATCH_STATUS_OVER = "over";
	public static final String TABLE_BATCH_STATUS_RUNNING = "running";

	@Deprecated
	private String offset;

	private Long eventTime;

	private Long eventSerialNo;

	private Long sourceTime;

	private String syncStage;
	@JsonIgnore
	@Deprecated
	private Object offsetObj;
	private String batchOffset;
	private String streamOffset;
	@JsonIgnore
	private Object batchOffsetObj;
	@JsonIgnore
	private Object streamOffsetObj;
	private Type type = Type.NORMAL;

	public SyncProgress() {
		this.eventSerialNo = 1L;
		this.eventTime = System.currentTimeMillis();
		this.sourceTime = System.currentTimeMillis();
		this.batchOffsetObj = new HashMap<>();
	}

	@Deprecated
	public String getOffset() {
		return offset;
	}

	@Deprecated
	public void setOffset(String offset) {
		this.offset = offset;
	}

	public Long getEventTime() {
		return eventTime;
	}

	public void setEventTime(Long eventTime) {
		this.eventTime = eventTime;
	}

	public String getSyncStage() {
		return syncStage;
	}

	public void setSyncStage(String syncStage) {
		this.syncStage = syncStage;
	}

	public Long getSourceTime() {
		return sourceTime;
	}

	public void setSourceTime(Long sourceTime) {
		this.sourceTime = sourceTime;
	}

	public Long getEventSerialNo() {
		return eventSerialNo;
	}

	public void setEventSerialNo(Long eventSerialNo) {
		this.eventSerialNo = eventSerialNo;
	}

	@JsonIgnore
	public boolean isEmpty() {
		return StringUtils.isEmpty(offset);
	}

	@Override
	public int compareTo(@NotNull SyncProgress o) {
		return Optional.ofNullable(getEventSerialNo()).orElse(0L).compareTo(
				Optional.ofNullable(o.getEventSerialNo()).orElse(0L)
		);
	}

	public long addAndGetSerialNo(int v) {
		return eventSerialNo += v;
	}

	public Object getOffsetObj() {
		return offsetObj;
	}

	public void setOffsetObj(Object offsetObj) {
		this.offsetObj = offsetObj;
	}

	public String getBatchOffset() {
		return batchOffset;
	}

	public void setBatchOffset(String batchOffset) {
		this.batchOffset = batchOffset;
	}

	public String getStreamOffset() {
		return streamOffset;
	}

	public void setStreamOffset(String streamOffset) {
		this.streamOffset = streamOffset;
	}

	public void setBatchOffsetObj(Object batchOffsetObj) {
		this.batchOffsetObj = batchOffsetObj;
	}

	public Object getStreamOffsetObj() {
		return streamOffsetObj;
	}

	public void setStreamOffsetObj(Object streamOffsetObj) {
		this.streamOffsetObj = streamOffsetObj;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public enum Type {
		NORMAL, // from normal task
		LOG_COLLECTOR, // from log collector task
		SHARE_CDC, // from share cdc task
		POLLING_CDC, // from polling cdc task
		UNIT_TEST,
		;
	}

	public boolean batchIsOverOfTable(String tableId) {
		if (null == batchOffsetObj) return false;
		Map<String, Object> offsetInfo = (Map<String, Object>)((Map<String, Object>) batchOffsetObj).get(tableId);
		if (null == offsetInfo) {
			return false;
		}
		return TABLE_BATCH_STATUS_OVER.equals(offsetInfo.get(STATUS));
	}

	public Object getBatchOffsetOfTable(String tableId) {
		if (null == batchOffsetObj) {
			return null;
		}
		Map<String, Object> offsetInfo = (Map<String, Object>)((Map<String, Object>) batchOffsetObj).get(tableId);
		if (null == offsetInfo) {
			return null;
		}
		return offsetInfo.get(OFFSET);
	}

	public void updateBatchOffset(String tableId, Object offset, String isOverTag) {
		Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) batchOffsetObj;
		if (null == batchOffsetObjTemp) {
			batchOffsetObjTemp = new HashMap<>();
			setBatchOffsetObj(batchOffsetObjTemp);
		}
		Map<String, Object> tableOffsetObjTemp = (Map<String, Object>)batchOffsetObjTemp.computeIfAbsent(tableId, k -> new HashMap<String, Object>());
		tableOffsetObjTemp.put(OFFSET, offset);
		tableOffsetObjTemp.put(STATUS, isOverTag);
	}

	public Object getBatchOffsetObj() {
		Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) batchOffsetObj;
		if (null == batchOffsetObjTemp) {
			batchOffsetObjTemp = new HashMap<>();
			setBatchOffsetObj(batchOffsetObjTemp);
		}

		return batchOffsetObj;
	}

	@Override
	public String toString() {
		return "SyncProgress{" +
				"eventSerialNo=" + eventSerialNo +
				", syncStage='" + syncStage + '\'' +
				", batchOffset='" + batchOffsetObj + '\'' +
				", streamOffset='" + streamOffsetObj + '\'' +
				'}';
	}
}

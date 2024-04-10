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
	public static final String TASK_BATCH_TABLE_OFFSET_POINT = "task_batch_table_offset_point";
	public static final String TASK_BATCH_TABLE_OFFSET_STATUS = "task_batch_table_offset_status";
	public static final String RUNNING = "running";

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
		Object offsetValue = getTableOffsetInfo(tableId);
		if (isBatchOffsetMap(offsetValue, TASK_BATCH_TABLE_OFFSET_STATUS)) {
			//86迭代新功能--全量表同步断点
			return TableBatchReadStatus.OVER.name().equals(((Map<?, ?>)offsetValue).get(TASK_BATCH_TABLE_OFFSET_STATUS));
		}
		//历史数据
		return false;
	}

	protected Object getTableOffsetInfo(String tableId) {
		if (batchOffsetObj instanceof Map) {
			return ((Map<?, ?>) batchOffsetObj).get(tableId);
		}
		return batchOffsetObj;
	}

	protected boolean isBatchOffsetMap(Object batchOffset, String key) {
		return batchOffset instanceof Map && ((Map<?, ?>)batchOffset).containsKey(key);
	}

	public Object getBatchOffsetOfTable(String tableId) {
		Object offsetValue = getTableOffsetInfo(tableId);
		if (isBatchOffsetMap(offsetValue, TASK_BATCH_TABLE_OFFSET_POINT)) {
			//86迭代新功能--全量表同步断点
			return ((Map<?, ?>) offsetValue).get(TASK_BATCH_TABLE_OFFSET_POINT);
		}

		//历史数据
		return offsetValue;
	}

	public void updateBatchOffset(String tableId, Object offset, String isOverTag) {
		Map<String, Object> batchOffsetObjTemp = putIfAbsentBatchOffsetObj();
		Map<String, Object> tableOffsetObjTemp = (Map<String, Object>) batchOffsetObjTemp.computeIfAbsent(tableId, k -> new HashMap<String, Object>());
		tableOffsetObjTemp.put(TASK_BATCH_TABLE_OFFSET_POINT, offset);
		tableOffsetObjTemp.put(TASK_BATCH_TABLE_OFFSET_STATUS, isOverTag);
	}

	public Map<String, Object> putIfAbsentBatchOffsetObj() {
		Map<String, Object> batchOffsetObjTemp = (Map<String, Object>) batchOffsetObj;
		if (null == batchOffsetObjTemp) {
			batchOffsetObjTemp = new HashMap<>();
			setBatchOffsetObj(batchOffsetObjTemp);
		}

		return batchOffsetObjTemp;
	}

	public Object getBatchOffsetObj() {
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

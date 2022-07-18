package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class TapdataOffset implements Serializable {

	private static final long serialVersionUID = 8734478335188595014L;

	public static final String SYNC_STAGE_SNAPSHOT = "snapshot";
	public static final String SYNC_STAGE_CDC = "cdc";
	public static final String SYNC_STAGE_FIELD_NAME = "syncStage";

	private String syncStage = SYNC_STAGE_SNAPSHOT;

	private Object offset;

	private long tapEventSerialNo;

	public TapdataOffset() {
	}

	public TapdataOffset(String syncStage, Object offset) {
		this.syncStage = syncStage;
		this.offset = offset;
	}

	public TapdataOffset(Object offset) {
		this.offset = offset;
	}

	public String getSyncStage() {
		return syncStage;
	}

	public void setSyncStage(String syncStage) {
		this.syncStage = syncStage;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

	public boolean empty() {
		return (StringUtils.isBlank(syncStage));
	}

	public long getTapEventSerialNo() {
		return tapEventSerialNo;
	}

	public void setTapEventSerialNo(long tapEventSerialNo) {
		this.tapEventSerialNo = tapEventSerialNo;
	}

	@Override
	public String toString() {
		return "TapdataOffset{" +
				"syncStage='" + syncStage + '\'' +
				", offset=" + offset +
				", tapEventSerialNo=" + tapEventSerialNo +
				'}';
	}
}

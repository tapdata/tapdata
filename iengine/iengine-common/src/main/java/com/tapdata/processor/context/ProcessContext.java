package com.tapdata.processor.context;

import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import org.apache.commons.collections.MapUtils;

import java.io.Serializable;
import java.util.Map;

public class ProcessContext implements Serializable {

	private static final long serialVersionUID = 7748501239988883921L;
	private String op;

	private String tableName;

	private Connections source;

	private Connections target;

	private Job job;

	private String syncType;

	private String type;

	private long eventTime;
	private long ts;

	private ProcessContextEvent event;

	public ProcessContext() {
	}

	public ProcessContext(String op, String tableName, Connections source, Connections target, Job job, Object offset) {
		this.op = op;
		this.tableName = tableName;
		this.source = source;
		this.target = target;
		this.job = job;

		convertOffsetToSyncType(offset);
	}

	public ProcessContext(ProcessContextEvent event) {
		this.event = event;
	}

	public void convertOffsetToSyncType(Object offset) {
		if (offset != null && offset instanceof TapdataOffset) {
			this.syncType = ((TapdataOffset) offset).getSyncStage();
		} else if (offset instanceof Map) {
			Map mapOffset = (Map) offset;
			if (MapUtils.isNotEmpty(mapOffset) && mapOffset.containsKey(TapdataOffset.SYNC_STAGE_FIELD_NAME)) {
				this.syncType = (String) mapOffset.get(TapdataOffset.SYNC_STAGE_FIELD_NAME);
			}
		}
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Connections getSource() {
		return source;
	}

	public void setSource(Connections source) {
		this.source = source;
	}

	public Connections getTarget() {
		return target;
	}

	public void setTarget(Connections target) {
		this.target = target;
	}

	public String getSyncType() {
		return syncType;
	}

	public void setSyncType(String syncType) {
		this.syncType = syncType;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public ProcessContextEvent getEvent() {
		return event;
	}

	public void setEvent(ProcessContextEvent event) {
		this.event = event;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}

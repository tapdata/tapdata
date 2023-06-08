package com.tapdata.entity;


import com.tapdata.entity.dataflow.SyncProgress;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 * @date 2021/8/12 4:52 PM
 **/
public class TapdataEvent implements Serializable, Cloneable {
	private static final long serialVersionUID = 2586329076282260051L;
	public final static String CONNECTION_ID_INFO_KEY = "connectionId";
	public final static String TABLE_NAMES_INFO_KEY = "tableNames";
	private SyncStage syncStage;

	private TapEvent tapEvent;

	private List<String> nodeIds;

	private Long sourceTime;

	private Long sourceSerialNo;

	private Object offset;
	private Object batchOffset;
	private Object streamOffset;
	protected SyncProgress.Type type;
	protected volatile Map<String, Object> info;
	private TapInsertRecordEvent exactlyOnceWriteCache;

	public TapdataEvent() {
		this.nodeIds = new ArrayList<>();
		this.type = SyncProgress.Type.NORMAL;
	}

	@Deprecated
	private MessageEntity messageEntity;

	/**
	 * 主从合并(MongoDB)，反查结果
	 */
	private Map<String, Object> mergeTableLookupResult;

	private String fromNodeId;

	public SyncStage getSyncStage() {
		return syncStage;
	}

	public void setSyncStage(SyncStage syncStage) {
		this.syncStage = syncStage;
	}
//
//  public Event getEvent() {
//    return event;
//  }
//
//  public void setEvent(Event event) {
//    this.event = event;
//  }

	@Deprecated
	public MessageEntity getMessageEntity() {
		return messageEntity;
	}

	@Deprecated
	public void setMessageEntity(MessageEntity messageEntity) {
		this.messageEntity = messageEntity;
	}

	public TapEvent getTapEvent() {
		return tapEvent;
	}

	public void setTapEvent(TapEvent tapEvent) {
		this.tapEvent = tapEvent;
	}

	public List<String> getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(List<String> nodeIds) {
		this.nodeIds = nodeIds;
	}

	public synchronized void addNodeId(String nodeId) {
		if (this.nodeIds == null) {
			this.nodeIds = new ArrayList<>();
		}
		if (!this.nodeIds.contains(nodeId)) {
			this.nodeIds.add(nodeId);
		}
	}

	public Long getSourceTime() {
		return sourceTime;
	}

	public void setSourceTime(Long sourceTime) {
		this.sourceTime = sourceTime;
	}

	public Long getSourceSerialNo() {
		return sourceSerialNo;
	}

	public void setSourceSerialNo(Long sourceSerialNo) {
		this.sourceSerialNo = sourceSerialNo;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

	public Map<String, Object> getMergeTableLookupResult() {
		return mergeTableLookupResult;
	}

	public void setMergeTableLookupResult(Map<String, Object> mergeTableLookupResult) {
		this.mergeTableLookupResult = mergeTableLookupResult;
	}

	public SyncProgress.Type getType() {
		return type;
	}

	public void setType(SyncProgress.Type type) {
		this.type = type;
	}

	public void setInfo(Map<String, Object> info) {
		this.info = info;
	}

	@Override
	public Object clone() {
		try {
			TapdataEvent obj = this.getClass().newInstance();
			clone(obj);
			return obj;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	protected void clone(TapdataEvent tapdataEvent) {
		tapdataEvent.setSourceTime(this.getSourceTime());
		tapdataEvent.setSourceTime(sourceTime);
		tapdataEvent.setSourceSerialNo(sourceSerialNo);
		tapdataEvent.setSyncStage(syncStage);
		tapdataEvent.setBatchOffset(batchOffset);
		tapdataEvent.setStreamOffset(streamOffset);
		tapdataEvent.setOffset(offset);
		tapdataEvent.setType(type);
		tapdataEvent.setMergeTableLookupResult(mergeTableLookupResult);
		tapdataEvent.setFromNodeId(fromNodeId);
		if (null != info) {
			tapdataEvent.setInfo(new HashMap<>(info));
		}
		if (this.getNodeIds() != null) {
			tapdataEvent.nodeIds = new ArrayList<>(this.getNodeIds());
		}

		if (messageEntity != null) {
			final MessageEntity cloneMessage = (MessageEntity) messageEntity.clone();
			tapdataEvent.setMessageEntity(cloneMessage);
		}

		if (tapEvent != null) {
			try {
				TapEvent cloneTapEvent = tapEvent.getClass().newInstance();
				tapEvent.clone(cloneTapEvent);
				tapdataEvent.setTapEvent(cloneTapEvent);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException("Clone tap event failed: " + e.getMessage(), e);
			}
		}
	}

	public String getFromNodeId() {
		return fromNodeId;
	}

	public void setFromNodeId(String fromNodeId) {
		this.fromNodeId = fromNodeId;
	}

	public Object getBatchOffset() {
		return batchOffset;
	}

	public void setBatchOffset(Object batchOffset) {
		this.batchOffset = batchOffset;
	}

	public Object getStreamOffset() {
		return streamOffset;
	}

	public void setStreamOffset(Object streamOffset) {
		this.streamOffset = streamOffset;
	}

	public boolean isDML() {
		return tapEvent instanceof TapRecordEvent;
	}

	public boolean isDDL() {
		return tapEvent instanceof TapDDLEvent;
	}

	public Object addInfo(String key, Object value) {
		initInfo();
		return info.put(key, value);
	}

	public void addInfos(Map<String, Object> map) {
		initInfo();
		info.putAll(map);
	}

	public Object getInfo(String key) {
		initInfo();
		return info.get(key);
	}

	public Object removeInfo(String key) {
		initInfo();
		return info.remove(key);
	}

	private void initInfo() {
		if (null == info) {
			synchronized (this) {
				if (null == info) {
					info = new LinkedHashMap<>();
				}
			}
		}
	}

	public TapInsertRecordEvent getExactlyOnceWriteCache() {
		return exactlyOnceWriteCache;
	}

	public void setExactlyOnceWriteCache(TapInsertRecordEvent exactlyOnceWriteCache) {
		this.exactlyOnceWriteCache = exactlyOnceWriteCache;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("TapdataEvent{");
		sb.append("syncStage=").append(syncStage);
		sb.append(", tapEvent=").append(tapEvent);
		sb.append(", nodeIds=").append(nodeIds);
		sb.append(", sourceTime=").append(sourceTime);
		sb.append(", sourceSerialNo=").append(sourceSerialNo);
		sb.append('}');
		return sb.toString();
	}
}

package com.tapdata.entity;

import com.tapdata.constant.ListUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.processor.context.ProcessContext;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tapdata on 20/06/2017.
 */
public class MessageEntity implements Serializable, Cloneable {

	private static final long serialVersionUID = 3180718023853919585L;

	private String op;

	private Map<String, Object> before;

	private Map<String, Object> after;

	private Map<String, Object> info;

	private List<Map<String, Object>> targetBefore;

	private String tableName;

	/**
	 * mongodb 3.2 或以下版本
	 */
	private Object opExpression;

	private Object offset;

	private String offsetJson;

	private Long timestamp;

	/**
	 * 消息目标节点id
	 */
	private String targetStageId;

	/**
	 * 消息经过的最后一个处理器的id
	 */
	private String processorStageId;

	/**
	 * 消息来源的节点id
	 */
	private String sourceStageId;

	/**
	 * OneMany sub data
	 */
	private Map<String, Map<String, Object>> subMap;

	/**
	 * for mongodb ns
	 */
	private String ns;

	private boolean isJdbc = true;

	/**
	 * for Data quality validate
	 */
	private DataQualityTag dataQualityTag;

	private ProcessContext processContext;

	private Mapping mapping;

	private List<ObjectId> gridfsObjectIds;

	/**
	 * record cdc event
	 */
	private Map<String, Object> cdcEvent;
	private SyncStageEnum syncStage = SyncStageEnum.UNKNOWN;
	private Set<String> updateFields;
	/**
	 * just for mongodb cdc event
	 */
	private Set<String> removeFields;

	private String ddl;

	/**
	 * 任务相关的消息的payload
	 */
	private JobMessagePayload jobMessagePayload;

	/**
	 * __tapd8字段数据
	 */
	private Map<String, Object> tapd8MetaData;

	private Long time;

	public MessageEntity() {
	}

	public static MessageEntity buildCommitOffsetMsg() {
		MessageEntity msg = new MessageEntity();
		msg.setOp(OperationType.COMMIT_OFFSET.getOp());
		return msg;
	}

	public MessageEntity(String op, Map<String, Object> after, String tableName) {
		this.op = op;
		this.after = after;
		this.tableName = tableName;
	}

	public MessageEntity(String op, Map<String, Object> after, String tableName, Mapping mapping) {
		this.op = op;
		this.after = after;
		this.tableName = tableName;
		this.mapping = mapping;
	}

	public MessageEntity(String op, Map<String, Object> after, String tableName, String targetStageId) {
		this.op = op;
		this.after = after;
		this.tableName = tableName;
		this.targetStageId = targetStageId;
	}

	/**
	 * 用于创建重试消息体
	 * <p>
	 * 通知目标线程，源端出错，进行重试，目标线程可以感知并作出相应的重置工作
	 *
	 * @return
	 */
	public static MessageEntity retryMessage() {
		MessageEntity messageEntity = new MessageEntity();
		messageEntity.setOp(OperationType.RETRY.getOp());
		return messageEntity;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public Map<String, Object> getAfter() {
		return after;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	public void setBefore(Map<String, Object> before) {
		this.before = before;
	}

	public Map<String, Object> getBefore() {
		return before;
	}

	public Map<String, Object> getInfo() {
		return info;
	}

	public void setInfo(Map<String, Object> info) {
		this.info = info;
	}

	public List<Map<String, Object>> getTargetBefore() {
		return targetBefore;
	}

	public void setTargetBefore(List<Map<String, Object>> targetBefore) {
		this.targetBefore = targetBefore;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Object getOpExpression() {
		return opExpression;
	}

	public void setOpExpression(Object opExpression) {
		this.opExpression = opExpression;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		if (timestamp != null && timestamp <= 0) {
			timestamp = null;
		}
		this.timestamp = timestamp;
	}

	public Map<String, Map<String, Object>> getSubMap() {
		return subMap;
	}

	public void setSubMap(Map<String, Map<String, Object>> subMap) {
		this.subMap = subMap;
	}

	public String getNs() {
		return ns;
	}

	public void setNs(String ns) {
		this.ns = ns;
	}

	public boolean getJdbc() {
		return isJdbc;
	}

	public void setJdbc(boolean jdbc) {
		isJdbc = jdbc;
	}

	public DataQualityTag getDataQualityTag() {
		return dataQualityTag;
	}

	public void setDataQualityTag(DataQualityTag dataQualityTag) {
		this.dataQualityTag = dataQualityTag;
	}

	public void removeDataQuality() {
		if (MapUtils.isNotEmpty(after)) {
			after.remove(DataQualityTag.SUB_COLUMN_NAME);
		}

		if (MapUtils.isNotEmpty(before)) {
			before.remove(DataQualityTag.SUB_COLUMN_NAME);
		}
	}

	public String getTargetStageId() {
		return targetStageId;
	}

	public void setTargetStageId(String targetStageId) {
		this.targetStageId = targetStageId;
	}

	public String getSourceStageId() {
		return sourceStageId;
	}

	public void setSourceStageId(String sourceStageId) {
		this.sourceStageId = sourceStageId;
	}

	public ProcessContext getProcessContext() {
		return processContext;
	}

	public void setProcessContext(ProcessContext processContext) {
		this.processContext = processContext;
	}

	public Mapping getMapping() {
		return mapping;
	}

	public void setMapping(Mapping mapping) {
		this.mapping = mapping;
	}

	public List<ObjectId> getGridfsObjectIds() {
		return gridfsObjectIds;
	}

	public void setGridfsObjectIds(List<ObjectId> gridfsObjectIds) {
		this.gridfsObjectIds = gridfsObjectIds;
	}

	public SyncStageEnum getSyncStage() {
		return syncStage;
	}

	public void setSyncStage(SyncStageEnum syncStage) {
		this.syncStage = syncStage;
	}

	public Set<String> getUpdateFields() {
		return updateFields;
	}

	public void setUpdateFields(Set<String> updateFields) {
		this.updateFields = updateFields;
	}

	public Map<String, Object> getCdcEvent() {
		return cdcEvent;
	}

	public void setCdcEvent(Map<String, Object> cdcEvent) {
		this.cdcEvent = cdcEvent;
	}

	public Set<String> getRemoveFields() {
		return removeFields;
	}

	public void setRemoveFields(Set<String> removeFields) {
		this.removeFields = removeFields;
	}

	public String getDdl() {
		return ddl;
	}

	public void setDdl(String ddl) {
		this.ddl = ddl;
	}

	public String getProcessorStageId() {
		return processorStageId;
	}

	public void setProcessorStageId(String processorStageId) {
		this.processorStageId = processorStageId;
	}

	public JobMessagePayload getJobMessagePayload() {
		return jobMessagePayload;
	}

	public void setJobMessagePayload(JobMessagePayload jobMessagePayload) {
		this.jobMessagePayload = jobMessagePayload;
	}

	public Map<String, Object> getTapd8MetaData() {
		return tapd8MetaData;
	}

	public void setTapd8MetaData(Map<String, Object> tapd8MetaData) {
		this.tapd8MetaData = tapd8MetaData;
	}

	public String getOffsetJson() {
		return offsetJson;
	}

	public void setOffsetJson(String offsetJson) {
		this.offsetJson = offsetJson;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	@Override
	public String toString() {
		return "MessageEntity{" +
				"op='" + op + '\'' +
				", before=" + before +
				", after=" + after +
				", targetBefore=" + targetBefore +
				", tableName='" + tableName + '\'' +
				", opExpression=" + opExpression +
				", offset=" + offset +
				", offsetJson='" + offsetJson + '\'' +
				", timestamp=" + timestamp +
				", targetStageId='" + targetStageId + '\'' +
				", processorStageId='" + processorStageId + '\'' +
				", sourceStageId='" + sourceStageId + '\'' +
				", subMap=" + subMap +
				", ns='" + ns + '\'' +
				", isJdbc=" + isJdbc +
				", dataQualityTag=" + dataQualityTag +
				", processContext=" + processContext +
				", mapping=" + mapping +
				", gridfsObjectIds=" + gridfsObjectIds +
				", cdcEvent=" + cdcEvent +
				", syncStage=" + syncStage +
				", updateFields=" + updateFields +
				", removeFields=" + removeFields +
				", ddl='" + ddl + '\'' +
				", jobMessagePayload=" + jobMessagePayload +
				", tapd8MetaData=" + tapd8MetaData +
				", time=" + time +
				'}';
	}

	@Override
	public Object clone() {
		MessageEntity newMSG = new MessageEntity();
		newMSG.setTableName(this.tableName);
		newMSG.setOp(this.op);
		newMSG.setTimestamp(this.timestamp);
		newMSG.setTime(this.time);
		newMSG.setDataQualityTag(this.dataQualityTag);
		newMSG.setJdbc(this.isJdbc);
		newMSG.setProcessContext(this.processContext);
		newMSG.setNs(this.ns);
		newMSG.setSubMap(this.subMap);
		newMSG.setMapping(this.mapping);
		newMSG.setSourceStageId(this.sourceStageId);
		newMSG.setTargetStageId(this.targetStageId);
		newMSG.setOffset(this.offset);
		newMSG.setOpExpression(this.opExpression);
		newMSG.setJobMessagePayload(this.jobMessagePayload);
		newMSG.setDdl(this.ddl);
		newMSG.setRemoveFields(this.removeFields);
		newMSG.setProcessorStageId(this.processorStageId);
		newMSG.setProcessContext(this.processContext);

		try {
			if (MapUtils.isNotEmpty(this.tapd8MetaData)) {
				Map newTapd8MetaData = this.tapd8MetaData.getClass().newInstance();
				MapUtil.deepCloneMap(this.tapd8MetaData, newTapd8MetaData);
				newMSG.setTapd8MetaData(newTapd8MetaData);
			}
			if (before != null) {
				Map newBefore = before.getClass().newInstance();
				MapUtil.deepCloneMap(before, newBefore);
				newMSG.setBefore(newBefore);
			}
			if (targetBefore != null) {
				List newTargetBefore = targetBefore.getClass().newInstance();
				ListUtil.serialCloneList(targetBefore, newTargetBefore);
				newMSG.setTargetBefore(newTargetBefore);
			}

			if (after != null) {
				Map newAfter = after.getClass().newInstance();
				MapUtil.deepCloneMap(after, newAfter);
				newMSG.setAfter(newAfter);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return newMSG;
	}

	public boolean isBeforeEmpty() {
		return MapUtils.isEmpty(before);
	}

	public boolean isDml() {
		if (StringUtils.isNotBlank(op) && OperationType.isDml(op)) {
			return true;
		}
		return false;
	}
}

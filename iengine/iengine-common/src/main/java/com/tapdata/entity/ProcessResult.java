package com.tapdata.entity;

import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.collections.MapUtils;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Map;

/**
 * Created by tapdata on 30/03/2018.
 */
public class ProcessResult {

	private Query query;

	private Update update;

	private String op;

	private String collectionName;

	private Object offset;

	private String fromTable;

	private int processSize;

	/**
	 * only for many one embed array
	 */
	private boolean array;

	private UpdateOptions updateOptions = new UpdateOptions().upsert(true);

	private String relationship;

	private WriteModel<Document> writeModel;

	private String targetpath;

	private DataQualityTag dataQualityTag;

	private long msgBatchNo;

	private Long timestamp;

	private String stageId;

	private Map<String, Object> after;

	private Map<String, Object> tapd8MetaData;

	private boolean arrayExists;

	public ProcessResult() {
	}

	public ProcessResult(ProcessResult processResult) {
		this.query = processResult.getQuery();
		this.update = processResult.getUpdate();
		this.op = processResult.getOp();
		this.collectionName = processResult.getCollectionName();
		this.offset = processResult.getOffset();
		this.fromTable = processResult.getFromTable();
		this.processSize = processResult.getProcessSize();
		this.array = processResult.isArray();
		this.updateOptions = processResult.getUpdateOptions();
		this.relationship = processResult.getRelationship();
		this.writeModel = processResult.getWriteModel();
		this.targetpath = processResult.getTargetpath();
		this.dataQualityTag = processResult.getDataQualityTag();
		this.msgBatchNo = processResult.getMsgBatchNo();
		this.timestamp = processResult.getTimestamp();
		this.stageId = processResult.getStageId();
		this.after = processResult.getAfter();
		this.tapd8MetaData = processResult.getTapd8MetaData();
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public Update getUpdate() {
		return update;
	}

	public void setUpdate(Update update) {
		this.update = update;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

	public String getFromTable() {
		return fromTable;
	}

	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}

	public int getProcessSize() {
		return processSize;
	}

	public void setProcessSize(int processSize) {
		this.processSize = processSize;
	}

	public String getRelationship() {
		return relationship;
	}

	public void setRelationship(String relationship) {
		this.relationship = relationship;
	}

	public WriteModel<Document> getWriteModel() {
		return writeModel;
	}

	public void setWriteModel(WriteModel<Document> writeModel) {
		this.writeModel = writeModel;
	}

	public String getTargetpath() {
		return targetpath;
	}

	public void setTargetpath(String targetpath) {
		this.targetpath = targetpath;
	}

	public DataQualityTag getDataQualityTag() {
		return dataQualityTag;
	}

	public void setDataQualityTag(DataQualityTag dataQualityTag) {
		this.dataQualityTag = dataQualityTag;
	}

	public long getMsgBatchNo() {
		return msgBatchNo;
	}

	public void setMsgBatchNo(long msgBatchNo) {
		this.msgBatchNo = msgBatchNo;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public UpdateOptions getUpdateOptions() {
		return updateOptions;
	}

	public void setUpdateOptions(UpdateOptions updateOptions) {
		this.updateOptions = updateOptions;
	}

	public boolean isArray() {
		return array;
	}

	public void setArray(boolean array) {
		this.array = array;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public Map<String, Object> getAfter() {
		return after;
	}

	public void setAfter(Map<String, Object> after) {
		this.after = after;
	}

	public Map<String, Object> getTapd8MetaData() {
		return tapd8MetaData;
	}

	public void setTapd8MetaData(Map<String, Object> tapd8MetaData) {
		this.tapd8MetaData = tapd8MetaData;
	}

	public boolean getArrayExists() {
		return arrayExists;
	}

	public void setArrayExists(boolean arrayExists) {
		this.arrayExists = arrayExists;
	}

	public void setTapd8MetaDataFromMsg(MessageEntity messageEntity) {
		final Map<String, Object> tapd8MetaData = messageEntity.getTapd8MetaData();
		if (MapUtils.isNotEmpty(tapd8MetaData)) {
			if (this.update == null) {
				throw new IllegalArgumentException("update operation cannot be null.");
			}

			for (Map.Entry<String, Object> entry : tapd8MetaData.entrySet()) {
				this.update.set(DataQualityTag.SUB_COLUMN_NAME + "." + entry.getKey(), entry.getValue());
			}
		}
	}

	@Override
	public String toString() {
		return "ProcessResult{" +
				"query=" + query +
				", update=" + update +
				", op='" + op + '\'' +
				", collectionName='" + collectionName + '\'' +
				", offset=" + offset +
				", fromTable='" + fromTable + '\'' +
				", processSize=" + processSize +
				", relationship='" + relationship + '\'' +
				", writeModel=" + writeModel +
				", targetpath='" + targetpath + '\'' +
				'}';
	}


}

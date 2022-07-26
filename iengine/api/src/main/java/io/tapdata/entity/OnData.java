package io.tapdata.entity;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.dataflow.RuntimeThroughput;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnData {

	Logger logger = LogManager.getLogger(getClass());

	private Object offset;
	private long source_received;
	private long processed;
	private long target_inserted;
	private long total_updated;
	private long total_deleted;
	/**
	 * dml事件条数
	 */
	private long dmlCount;

	// only for file(s) target
	private long total_file_length;

	private long total_data_quality;

	private List<MessageEntity> msgs;

	private Map<String, RuntimeThroughput> insertStage = new HashMap<>();

	private Map<String, RuntimeThroughput> updateStage = new HashMap<>();

	private Map<String, RuntimeThroughput> deleteStage = new HashMap<>();

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

	public long getSource_received() {
		return source_received;
	}

	public void setSource_received(long source_received) {
		this.source_received = source_received;
	}

	public long getProcessed() {
		return processed;
	}

	public void setProcessed(long processed) {
		this.processed = processed;
	}

	public void increaseProcessed(long processed) {
		this.processed += processed;
	}

	public void increaseInserted(long target_inserted) {
		this.target_inserted += target_inserted;
	}

	public void increaseDeleted(long total_deleted) {
		this.total_deleted += total_deleted;
	}

	public void increaseUpdated(long total_updated) {
		this.total_updated += total_updated;
	}

	public long getTarget_inserted() {
		return target_inserted;
	}

	public void setTarget_inserted(long target_inserted) {
		this.target_inserted = target_inserted;
	}

	public long getTotal_updated() {
		return total_updated;
	}

	public void setTotal_updated(long total_updated) {
		this.total_updated = total_updated;
	}

	public long getTotal_deleted() {
		return total_deleted;
	}

	public void setTotal_deleted(long total_deleted) {
		this.total_deleted = total_deleted;
	}

	public long getTotal_file_length() {
		return total_file_length;
	}

	public void setTotal_file_length(long total_file_length) {
		this.total_file_length = total_file_length;
	}

	public long getTotal_data_quality() {
		return total_data_quality;
	}

	public void setTotal_data_quality(long total_data_quality) {
		this.total_data_quality = total_data_quality;
	}

	public Map<String, RuntimeThroughput> getInsertStage() {
		return insertStage;
	}

	public Map<String, RuntimeThroughput> getUpdateStage() {
		return updateStage;
	}

	public Map<String, RuntimeThroughput> getDeleteStage() {
		return deleteStage;
	}

	public void setInsertStage(Map<String, RuntimeThroughput> insertStage) {
		this.insertStage = insertStage;
	}

	public void setUpdateStage(Map<String, RuntimeThroughput> updateStage) {
		this.updateStage = updateStage;
	}

	public void setDeleteStage(Map<String, RuntimeThroughput> deleteStage) {
		this.deleteStage = deleteStage;
	}

	public List<MessageEntity> getMsgs() {
		return msgs;
	}

	public void setMsgs(List<MessageEntity> msgs) {
		this.msgs = msgs;
	}

	public void incrementStatisticsStage(MessageEntity msg) {
		switch (msg.getOp()) {
			case ConnectorConstant.MESSAGE_OPERATION_INSERT:
				incrementMdfStage(insertStage, msg);
				break;
			case ConnectorConstant.MESSAGE_OPERATION_UPDATE:
				incrementMdfStage(updateStage, msg);
				break;
			case ConnectorConstant.MESSAGE_OPERATION_DELETE:
				incrementMdfStage(deleteStage, msg);
				break;
			default:
				if (logger.isDebugEnabled()) {
					logger.debug("op [{}] statistics ignore...", msg.getOp());
				}
				break;
		}
	}

	public void incrementMdfStage(Map<String, RuntimeThroughput> mdfStage, MessageEntity msg) {
		String stageId = msg.getTargetStageId();
		if (StringUtils.isBlank(stageId)) {
			return;
		}
		if (!mdfStage.containsKey(stageId)) {
			mdfStage.put(stageId, new RuntimeThroughput());
		}
		mdfStage.get(stageId).incrementRows(1);
	}

	public void incrementCountInsertStage(String stageId, long incrementCount) {
		increaseInserted(incrementCount);
		if (!insertStage.containsKey(stageId)) {
			insertStage.put(stageId, new RuntimeThroughput());
		}
		insertStage.get(stageId).incrementRows(incrementCount);
	}

	public void incrementCountUpdateStage(String stageId, long incrementCount) {
		increaseUpdated(incrementCount);
		if (!updateStage.containsKey(stageId)) {
			updateStage.put(stageId, new RuntimeThroughput());
		}
		updateStage.get(stageId).incrementRows(incrementCount);
	}

	public void incrementCountDeleteStage(String stageId, long incrementCount) {
		increaseDeleted(incrementCount);
		if (!deleteStage.containsKey(stageId)) {
			deleteStage.put(stageId, new RuntimeThroughput());
		}
		deleteStage.get(stageId).incrementRows(incrementCount);
	}

	public long getDmlCount() {
		return dmlCount;
	}

	public void setDmlCount(long dmlCount) {
		this.dmlCount = dmlCount;
	}

	public void setDmlCount(List<MessageEntity> msgs) {
		setDmlCount(0L);
		for (MessageEntity msg : msgs) {
			if (OperationType.isDml(msg.getOp())) {
				this.dmlCount++;
			}
		}
	}

	public long collectDmlRowState() {
		Long insert = insertStage.values().stream().mapToLong(RuntimeThroughput::getRows).sum();
		Long update = updateStage.values().stream().mapToLong(RuntimeThroughput::getRows).sum();
		Long delete = deleteStage.values().stream().mapToLong(RuntimeThroughput::getRows).sum();
		return insert + update + delete;
	}

	/**
	 * 该批次的dml事件是否处理完成
	 *
	 * @return
	 */
	public boolean isProcessed() {
		return collectDmlRowState() == this.dmlCount;
	}
}

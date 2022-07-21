package com.tapdata.entity;

import com.mongodb.bulk.BulkWriteResult;

/**
 * Created by tapdata on 01/04/2018.
 */
public class BulkOpResult {

	private BulkWriteResult bulkWriteResult;

	private Integer processSize;

	private Long totalDataQuality;

	private String stageId;

	private String targetConnectionId;

	private String targetCollectionName;

	public BulkOpResult(BulkWriteResult bulkWriteResult, Integer processSize) {
		this.bulkWriteResult = bulkWriteResult;
		this.processSize = processSize;
	}

	public BulkOpResult(BulkWriteResult bulkWriteResult, Integer processSize, Long totalDataQuality) {
		this.bulkWriteResult = bulkWriteResult;
		this.processSize = processSize;
		this.totalDataQuality = totalDataQuality;
	}

	public BulkWriteResult getBulkWriteResult() {
		return bulkWriteResult;
	}

	public void setBulkWriteResult(BulkWriteResult bulkWriteResult) {
		this.bulkWriteResult = bulkWriteResult;
	}

	public Integer getProcessSize() {
		return processSize;
	}

	public void setProcessSize(Integer processSize) {
		this.processSize = processSize;
	}

	public Long getTotalDataQuality() {
		return totalDataQuality;
	}

	public void setTotalDataQuality(Long totalDataQuality) {
		this.totalDataQuality = totalDataQuality;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public String getTargetConnectionId() {
		return targetConnectionId;
	}

	public void setTargetConnectionId(String targetConnectionId) {
		this.targetConnectionId = targetConnectionId;
	}

	public String getTargetCollectionName() {
		return targetCollectionName;
	}

	public void setTargetCollectionName(String targetCollectionName) {
		this.targetCollectionName = targetCollectionName;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("BulkOpResult{");
		sb.append("bulkWriteResult=").append(bulkWriteResult);
		sb.append(", processSize=").append(processSize);
		sb.append('}');
		return sb.toString();
	}
}

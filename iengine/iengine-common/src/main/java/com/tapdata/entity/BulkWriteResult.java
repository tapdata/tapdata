package com.tapdata.entity;

public class BulkWriteResult {

	private Long totalDataQuality;

	private com.mongodb.bulk.BulkWriteResult bulkWriteResult;

	private int error;

	public Long getTotalDataQuality() {
		return totalDataQuality;
	}

	public void setTotalDataQuality(Long totalDataQuality) {
		this.totalDataQuality = totalDataQuality;
	}

	public com.mongodb.bulk.BulkWriteResult getBulkWriteResult() {
		return bulkWriteResult;
	}

	public void setBulkWriteResult(com.mongodb.bulk.BulkWriteResult bulkWriteResult) {
		this.bulkWriteResult = bulkWriteResult;
	}

	public int getError() {
		return error;
	}

	public void setError(int error) {
		this.error = error;
	}
}

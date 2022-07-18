package io.tapdata.entity;

import com.tapdata.entity.Schema;

import java.util.List;

public class BaseConnectionValidateResult {

	public static final String CONNECTION_STATUS_INVALID = "invalid";
	public static final String CONNECTION_STATUS_READY = "ready";

	private int retry;

	private Long nextRetry;

	private String status;

	private List<BaseConnectionValidateResultDetail> validateResultDetails;

	private Schema schema;

	private Integer db_version;

	public int getRetry() {
		return retry;
	}

	public void setRetry(int retry) {
		this.retry = retry;
	}

	public Long getNextRetry() {
		return nextRetry;
	}

	public void setNextRetry(Long nextRetry) {
		this.nextRetry = nextRetry;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public List<BaseConnectionValidateResultDetail> getValidateResultDetails() {
		return validateResultDetails;
	}

	public void setValidateResultDetails(List<BaseConnectionValidateResultDetail> validateResultDetails) {
		this.validateResultDetails = validateResultDetails;
	}

	public Integer getDb_version() {
		return db_version;
	}

	public void setDb_version(Integer db_version) {
		this.db_version = db_version;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}
}

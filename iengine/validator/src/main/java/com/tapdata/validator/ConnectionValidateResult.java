package com.tapdata.validator;

import com.tapdata.entity.Schema;
import io.tapdata.pdk.apis.entity.ConnectionOptions;

import java.util.List;
import java.util.Map;

public class ConnectionValidateResult {

	private int retry;

	private Long nextRetry;

	private String status;

	private List<ConnectionValidateResultDetail> validateResultDetails;

	private Schema schema;

	private Integer db_version;

	private String dbFullVersion;
	private ConnectionOptions connectionOptions;

	private Map<String, Object> monitorResult;

	public ConnectionValidateResult() {
	}

	public ConnectionValidateResult(List<ConnectionValidateResultDetail> validateResultDetails) {
		this.validateResultDetails = validateResultDetails;
	}

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

	public List<ConnectionValidateResultDetail> getValidateResultDetails() {
		return validateResultDetails;
	}

	public void setValidateResultDetails(List<ConnectionValidateResultDetail> validateResultDetails) {
		this.validateResultDetails = validateResultDetails;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
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

	public String getDbFullVersion() {
		return dbFullVersion;
	}

	public void setDbFullVersion(String dbFullVersion) {
		this.dbFullVersion = dbFullVersion;
	}

	public ConnectionOptions getConnectionOptions() {
		return connectionOptions;
	}

	public void setConnectionOptions(ConnectionOptions connectionOptions) {
		this.connectionOptions = connectionOptions;
	}

	public Map<String, Object> getMonitorResult() {
		return monitorResult;
	}

	public void setMonitorResult(Map<String, Object> monitorResult) {
		this.monitorResult = monitorResult;
	}
}

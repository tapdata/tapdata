/**
 * @title: DataFlowSetting
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import java.util.List;

public class DataFlowSetting {

	private String sync_type;

	private Long readBatchSize;

	private Long readCdcInterval;

	private Long notificationWindow;

	private Long notificationInterval;

	private Boolean needToCreateIndex;

	private Boolean isSchedule;

	private String cronExpression;

	private Boolean stopOnError;

	private DataFlowEmailWarning emailWaring;

	private Boolean increment;

	private Boolean isOpenAutoDDL;

	private String syncPoint;

	private String syncTime;

	private Integer processorConcurrency;

	private Integer transformerConcurrency;

	private String discardDDL;

	private List<SyncPoints> syncPoints;

	private String distinctWriteType;

	private Double maxTransactionLength;

	private Boolean isSerialMode;

	private Integer cdcFetchSize;

	private ReadShareLogMode readShareLogMode;

	private Boolean cdcConcurrency;

	private Boolean cdcShareFilterOnServer;

	private Boolean noPrimaryKey;

	private String flowEngineVersion;

	public String getSync_type() {
		return sync_type;
	}

	public Long getReadBatchSize() {
		return readBatchSize;
	}

	public Long getReadCdcInterval() {
		return readCdcInterval;
	}

	public Long getNotificationWindow() {
		return notificationWindow;
	}

	public Long getNotificationInterval() {
		return notificationInterval;
	}

	public Boolean getNeedToCreateIndex() {
		return needToCreateIndex;
	}

	public Boolean getSchedule() {
		return isSchedule;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public Boolean getStopOnError() {
		return stopOnError;
	}

	public DataFlowEmailWarning getEmailWaring() {
		return emailWaring;
	}

	public Boolean getIncrement() {
		return increment;
	}

	public Boolean getOpenAutoDDL() {
		return isOpenAutoDDL;
	}

	public String getSyncPoint() {
		return syncPoint;
	}

	public String getSyncTime() {
		return syncTime;
	}

	public Integer getProcessorConcurrency() {
		return processorConcurrency;
	}

	public Integer getTransformerConcurrency() {
		return transformerConcurrency;
	}

	public String getDiscardDDL() {
		return discardDDL;
	}

	public List<SyncPoints> getSyncPoints() {
		return syncPoints;
	}

	public String getDistinctWriteType() {
		return distinctWriteType;
	}

	public Double getMaxTransactionLength() {
		return maxTransactionLength;
	}

	public Boolean getSerialMode() {
		return isSerialMode;
	}

	public Integer getCdcFetchSize() {
		return cdcFetchSize;
	}

	public ReadShareLogMode getReadShareLogMode() {
		return readShareLogMode;
	}

	public Boolean getCdcConcurrency() {
		return cdcConcurrency;
	}

	public Boolean getCdcShareFilterOnServer() {
		return cdcShareFilterOnServer;
	}

	public Boolean getNoPrimaryKey() {
		return noPrimaryKey;
	}

	public String getFlowEngineVersion() {
		return flowEngineVersion;
	}

	public void setSync_type(String sync_type) {
		this.sync_type = sync_type;
	}

	public void setReadBatchSize(Long readBatchSize) {
		this.readBatchSize = readBatchSize;
	}

	public void setReadCdcInterval(Long readCdcInterval) {
		this.readCdcInterval = readCdcInterval;
	}

	public void setNotificationWindow(Long notificationWindow) {
		this.notificationWindow = notificationWindow;
	}

	public void setNotificationInterval(Long notificationInterval) {
		this.notificationInterval = notificationInterval;
	}

	public void setNeedToCreateIndex(Boolean needToCreateIndex) {
		this.needToCreateIndex = needToCreateIndex;
	}

	public void setSchedule(Boolean schedule) {
		isSchedule = schedule;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public void setStopOnError(Boolean stopOnError) {
		this.stopOnError = stopOnError;
	}

	public void setEmailWaring(DataFlowEmailWarning emailWaring) {
		this.emailWaring = emailWaring;
	}

	public void setIncrement(Boolean increment) {
		this.increment = increment;
	}

	public void setOpenAutoDDL(Boolean openAutoDDL) {
		isOpenAutoDDL = openAutoDDL;
	}

	public void setSyncPoint(String syncPoint) {
		this.syncPoint = syncPoint;
	}

	public void setSyncTime(String syncTime) {
		this.syncTime = syncTime;
	}

	public void setProcessorConcurrency(Integer processorConcurrency) {
		this.processorConcurrency = processorConcurrency;
	}

	public void setTransformerConcurrency(Integer transformerConcurrency) {
		this.transformerConcurrency = transformerConcurrency;
	}

	public void setDiscardDDL(String discardDDL) {
		this.discardDDL = discardDDL;
	}

	public void setSyncPoints(List<SyncPoints> syncPoints) {
		this.syncPoints = syncPoints;
	}

	public void setDistinctWriteType(String distinctWriteType) {
		this.distinctWriteType = distinctWriteType;
	}

	public void setMaxTransactionLength(Double maxTransactionLength) {
		this.maxTransactionLength = maxTransactionLength;
	}

	public void setSerialMode(Boolean serialMode) {
		isSerialMode = serialMode;
	}

	public void setCdcFetchSize(Integer cdcFetchSize) {
		this.cdcFetchSize = cdcFetchSize;
	}

	public void setReadShareLogMode(ReadShareLogMode readShareLogMode) {
		this.readShareLogMode = readShareLogMode;
	}

	public void setCdcConcurrency(Boolean cdcConcurrency) {
		this.cdcConcurrency = cdcConcurrency;
	}

	public void setCdcShareFilterOnServer(Boolean cdcShareFilterOnServer) {
		this.cdcShareFilterOnServer = cdcShareFilterOnServer;
	}

	public void setNoPrimaryKey(Boolean noPrimaryKey) {
		this.noPrimaryKey = noPrimaryKey;
	}

	public void setFlowEngineVersion(String flowEngineVersion) {
		this.flowEngineVersion = flowEngineVersion;
	}
}

package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.FlowEngineVersion;
import com.tapdata.entity.SyncPoints;

import java.io.Serializable;
import java.util.List;

/**
 * @author jackin
 */
public class DataFlowSetting implements Serializable {

	private static final long serialVersionUID = -4620232083920178647L;
	private String sync_type = ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC;

	private long readBatchSize = 25000;

	private long readCdcInterval = 500;

	private long notificationWindow;

	private long notificationInterval;

	private boolean needToCreateIndex;

	private boolean isSchedule;

	private String cronExpression;

	private boolean stopOnError;

	private DataFlowEmailWarning emailWaring;

	private boolean increment;

	private boolean isOpenAutoDDL;

	private String syncPoint;

	private String syncTime;

	private int processorConcurrency = 1;

	private int transformerConcurrency = 8;

	private String discardDDL;

	private List<SyncPoints> syncPoints;

	private String distinctWriteType;

	private Double maxTransactionLength = 12d;

	private boolean isSerialMode;

	private int cdcFetchSize = 1;

	private ReadShareLogMode readShareLogMode;

	private boolean cdcConcurrency = false;

	private int manuallyMinerConcurrency = 1;

	private boolean cdcShareFilterOnServer = false; // 是否在服务端过滤（增量共享挖掘日志）

	/**
	 * 是否开启无主键同步
	 */
	private boolean noPrimaryKey;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private String flowEngineVersion = FlowEngineVersion.V1.getVersion();

	private String oracleLogminer = "automatically";

	/**
	 * 使用自定义SQL解析，默认：开启
	 */
	private boolean useCustomSQLParser = false;

	private String transformModelVersion;

	public long getReadBatchSize() {
		return readBatchSize;
	}

	public void setReadBatchSize(long readBatchSize) {
		this.readBatchSize = readBatchSize;
	}

	public long getReadCdcInterval() {
		return readCdcInterval;
	}

	public void setReadCdcInterval(long readCdcInterval) {
		this.readCdcInterval = readCdcInterval;
	}

	public long getNotificationWindow() {
		return notificationWindow;
	}

	public void setNotificationWindow(long notificationWindow) {
		this.notificationWindow = notificationWindow;
	}

	public long getNotificationInterval() {
		return notificationInterval;
	}

	public void setNotificationInterval(long notificationInterval) {
		this.notificationInterval = notificationInterval;
	}

	public boolean getNeedToCreateIndex() {
		return needToCreateIndex;
	}

	public void setNeedToCreateIndex(boolean needToCreateIndex) {
		this.needToCreateIndex = needToCreateIndex;
	}

	public boolean getIsSchedule() {
		return isSchedule;
	}

	public void setIsSchedule(boolean schedule) {
		isSchedule = schedule;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public DataFlowEmailWarning getEmailWaring() {
		return emailWaring;
	}

	public void setEmailWaring(DataFlowEmailWarning emailWaring) {
		this.emailWaring = emailWaring;
	}

	public String getSync_type() {
		return sync_type;
	}

	public void setSync_type(String sync_type) {
		this.sync_type = sync_type;
	}

	public boolean getStopOnError() {
		return stopOnError;
	}

	public void setStopOnError(boolean stopOnError) {
		this.stopOnError = stopOnError;
	}

	public boolean getIncrement() {
		return increment;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	public boolean getIsOpenAutoDDL() {
		return isOpenAutoDDL;
	}

	public void setIsOpenAutoDDL(boolean openAutoDDL) {
		isOpenAutoDDL = openAutoDDL;
	}

	public String getSyncPoint() {
		return syncPoint;
	}

	public void setSyncPoint(String syncPoint) {
		this.syncPoint = syncPoint;
	}

	public String getSyncTime() {
		return syncTime;
	}

	public void setSyncTime(String syncTime) {
		this.syncTime = syncTime;
	}

	public int getProcessorConcurrency() {
		return processorConcurrency;
	}

	public void setProcessorConcurrency(int processorConcurrency) {
		this.processorConcurrency = processorConcurrency;
	}

	public int getTransformerConcurrency() {
		return transformerConcurrency;
	}

	public void setTransformerConcurrency(int transformerConcurrency) {
		this.transformerConcurrency = transformerConcurrency;
	}

	public String getDiscardDDL() {
		return discardDDL;
	}

	public void setDiscardDDL(String discardDDL) {
		this.discardDDL = discardDDL;
	}

	public List<SyncPoints> getSyncPoints() {
		return syncPoints;
	}

	public void setSyncPoints(List<SyncPoints> syncPoints) {
		this.syncPoints = syncPoints;
	}

	public String getDistinctWriteType() {
		return distinctWriteType;
	}

	public void setDistinctWriteType(String distinctWriteType) {
		this.distinctWriteType = distinctWriteType;
	}

	public Double getMaxTransactionLength() {
		return maxTransactionLength;
	}

	public void setMaxTransactionLength(Double maxTransactionLength) {
		this.maxTransactionLength = maxTransactionLength;
	}

	public boolean getIsSerialMode() {
		return isSerialMode;
	}

	public void setIsSerialMode(boolean serialMode) {
		isSerialMode = serialMode;
	}

	public int getCdcFetchSize() {
		return cdcFetchSize;
	}

	public void setCdcFetchSize(int cdcFetchSize) {
		this.cdcFetchSize = cdcFetchSize;
	}

	public ReadShareLogMode getReadShareLogMode() {
		return readShareLogMode;
	}

	public boolean getCdcConcurrency() {
		return cdcConcurrency;
	}

	public void setCdcConcurrency(boolean cdcConcurrency) {
		this.cdcConcurrency = cdcConcurrency;
	}

	public boolean getCdcShareFilterOnServer() {
		return cdcShareFilterOnServer;
	}

	public void setCdcShareFilterOnServer(boolean cdcShareFilterOnServer) {
		this.cdcShareFilterOnServer = cdcShareFilterOnServer;
	}

	public boolean getNoPrimaryKey() {
		return noPrimaryKey;
	}

	public void setNoPrimaryKey(boolean noPrimaryKey) {
		this.noPrimaryKey = noPrimaryKey;
	}

	public String getFlowEngineVersion() {
		return flowEngineVersion;
	}

	public void setFlowEngineVersion(String flowEngineVersion) {
		this.flowEngineVersion = flowEngineVersion;
	}

	public String getOracleLogminer() {
		return oracleLogminer;
	}

	public void setOracleLogminer(String oracleLogminer) {
		this.oracleLogminer = oracleLogminer;
	}

	public boolean getUseCustomSQLParser() {
		return useCustomSQLParser;
	}

	public void setUseCustomSQLParser(boolean useCustomSQLParser) {
		this.useCustomSQLParser = useCustomSQLParser;
	}

	public String getTransformModelVersion() {
		return transformModelVersion;
	}

	public void setTransformModelVersion(String transformModelVersion) {
		this.transformModelVersion = transformModelVersion;
	}

	public int getManuallyMinerConcurrency() {
		return manuallyMinerConcurrency;
	}

	public void setManuallyMinerConcurrency(int manuallyMinerConcurrency) {
		this.manuallyMinerConcurrency = manuallyMinerConcurrency;
	}
}

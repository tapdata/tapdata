package com.tapdata.entity;

import java.util.Map;

/**
 * Created by tapdata on 20/03/2018.
 */
public class InconsistentData {

	public static final String VALIDATE_RESULT_ERROR = "error";

	public static final String VALIDATE_RESULT_RETRY = "retry";

	private String id;

	private String verificationJobId;

	private Map<String, Object> sourceRecord;

	private Map<String, Object> targetRecord;

	private long nextValidateTime;

	private Object eventRecord;

	private String validateResult = VALIDATE_RESULT_RETRY;

	private long createTime = System.currentTimeMillis();

	public InconsistentData() {
	}

	public InconsistentData(String verificationJobId, Map<String, Object> sourceRecord, long nextValidateTime, Object eventRecord, Map<String, Object> targetRecord) {
		this.verificationJobId = verificationJobId;
		this.sourceRecord = sourceRecord;
		this.nextValidateTime = nextValidateTime;
		this.eventRecord = eventRecord;
		this.targetRecord = targetRecord;
	}

	public InconsistentData(ValidateData validateData, String verificationJobId) {
		Map<String, Object> data = validateData.getData();
		this.sourceRecord = data;

		this.verificationJobId = verificationJobId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getVerificationJobId() {
		return verificationJobId;
	}

	public void setVerificationJobId(String verificationJobId) {
		this.verificationJobId = verificationJobId;
	}

	public long getNextValidateTime() {
		return nextValidateTime;
	}

	public void setNextValidateTime(long nextValidateTime) {
		this.nextValidateTime = nextValidateTime;
	}

	public String getValidateResult() {
		return validateResult;
	}

	public void setValidateResult(String validateResult) {
		this.validateResult = validateResult;
	}

	public Object getEventRecord() {
		return eventRecord;
	}

	public void setEventRecord(Object eventRecord) {
		this.eventRecord = eventRecord;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public Map<String, Object> getSourceRecord() {
		return sourceRecord;
	}

	public void setSourceRecord(Map<String, Object> sourceRecord) {
		this.sourceRecord = sourceRecord;
	}

	public Map<String, Object> getTargetRecord() {
		return targetRecord;
	}

	public void setTargetRecord(Map<String, Object> targetRecord) {
		this.targetRecord = targetRecord;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("InconsistentData{");
		sb.append("id=").append(id);
		sb.append(", verificationJobId='").append(verificationJobId).append('\'');
		sb.append(", sourceRecord=").append(sourceRecord);
		sb.append(", nextValidateTime=").append(nextValidateTime);
		sb.append('}');
		return sb.toString();
	}

	public static void main(String[] args) {
		String str1 = "{CUSTOMER_ID=79, EMPLOYEE_ID=2, FREIGHT=62.22, ORDER_DATE=1998-03-23T00:00:00.000+0800, ORDER_ID=10967, REQUIRED_DATE=1998-04-20T00:00:00.000+0800, SHIPPED_DATE=1998-04-02T00:00:00.000+0800, SHIP_ADDRESS=Luisenstr. 48, SHIP_CITY=Münster, SHIP_COUNTRY=Germany, SHIP_NAME=Toms Spezialitäten, SHIP_POSTAL_CODE=44087, SHIP_REGION=null, SHIP_VIA=2}";
		String str2 = "{CUSTOMER_ID=79, EMPLOYEE_ID=2, FREIGHT=62.22, ORDER_DATE=1998-03-23T00:00:00.000+0800, ORDER_ID=10967, REQUIRED_DATE=1998-04-20T00:00:00.000+0800, SHIPPED_DATE=1998-04-02T00:00:00.000+0800, SHIP_ADDRESS=Luisenstr. 48, SHIP_CITY=Münster, SHIP_COUNTRY=Germany, SHIP_NAME=Toms Spezialitäten, SHIP_POSTAL_CODE=44087, SHIP_REGION=null, SHIP_VIA=2}";
		System.out.println(str1.equals(str2));
	}
}

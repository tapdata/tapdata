package com.tapdata.entity;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

public class DataValidateResult implements Serializable {

	private String sourceTableName;
	private String sourceTablePK;
	private String targetTableName;
	private String targetTablePK;
	private long sourceCount;
	private long targetCount;
	private List<FieldProcess> fieldProcesses;
	private String script;
	private long total;
	private long passed;
	private long failed;
	private String passRate;
	private String failRate;

	public long getSourceCount() {
		return sourceCount;
	}

	public void setSourceCount(long sourceCount) {
		this.sourceCount = sourceCount;
	}

	public long getTargetCount() {
		return targetCount;
	}

	public void setTargetCount(long targetCount) {
		this.targetCount = targetCount;
	}

	public String getSourceTableName() {
		return sourceTableName;
	}

	public void setSourceTableName(String sourceTableName) {
		this.sourceTableName = sourceTableName;
	}

	public String getSourceTablePK() {
		return sourceTablePK;
	}

	public void setSourceTablePK(String sourceTablePK) {
		this.sourceTablePK = sourceTablePK;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public String getTargetTablePK() {
		return targetTablePK;
	}

	public void setTargetTablePK(String targetTablePK) {
		this.targetTablePK = targetTablePK;
	}

	public List<FieldProcess> getFieldProcesses() {
		return fieldProcesses;
	}

	public void setFieldProcesses(List<FieldProcess> fieldProcesses) {
		this.fieldProcesses = fieldProcesses;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public long getPassed() {
		return passed;
	}

	public void setPassed(long passed) {
		this.passed = passed;
	}

	public long getFailed() {
		return failed;
	}

	public void setFailed(long failed) {
		this.failed = failed;
	}

	public String getPassRate() {
		return passRate;
	}

	public void setPassRate(String passRate) {
		this.passRate = passRate;
	}

	public String getFailRate() {
		return failRate;
	}

	public void setFailRate(String failRate) {
		this.failRate = failRate;
	}

	public void increaseCount(long total, long passed, long failed) {
		this.total += total;
		this.passed += passed;
		this.failed += failed;

		DecimalFormat df = new DecimalFormat("0.00");
		this.passRate = df.format(((double) this.passed / this.total) * 100) + "%";
		this.failRate = df.format(((double) this.failed / this.total) * 100) + "%";
	}

}

/**
 * @title: Overview
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class Overview {
	
	private Integer sourceTableNum;
	
	private Long sourceRowNum;
	
	private Integer targetTableNum;
	
	private Long targatRowNum;
	
	private Long spendTime;
	
	private Integer waitingForSyecTableNums;
	
	private DataFlowProgressStatus status;

	public Integer getSourceTableNum() {
		return sourceTableNum;
	}

	public Long getSourceRowNum() {
		return sourceRowNum;
	}

	public Integer getTargetTableNum() {
		return targetTableNum;
	}

	public Long getTargatRowNum() {
		return targatRowNum;
	}

	public Long getSpendTime() {
		return spendTime;
	}

	public Integer getWaitingForSyecTableNums() {
		return waitingForSyecTableNums;
	}

	public DataFlowProgressStatus getStatus() {
		return status;
	}

	public void setSourceTableNum(Integer sourceTableNum) {
		this.sourceTableNum = sourceTableNum;
	}

	public void setSourceRowNum(Long sourceRowNum) {
		this.sourceRowNum = sourceRowNum;
	}

	public void setTargetTableNum(Integer targetTableNum) {
		this.targetTableNum = targetTableNum;
	}

	public void setTargatRowNum(Long targatRowNum) {
		this.targatRowNum = targatRowNum;
	}

	public void setSpendTime(Long spendTime) {
		this.spendTime = spendTime;
	}

	public void setWaitingForSyecTableNums(Integer waitingForSyecTableNums) {
		this.waitingForSyecTableNums = waitingForSyecTableNums;
	}

	public void setStatus(DataFlowProgressStatus status) {
		this.status = status;
	}
}

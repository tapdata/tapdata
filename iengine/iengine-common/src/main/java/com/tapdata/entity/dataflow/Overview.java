package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2021-07-12 14:15
 **/
public class Overview implements Serializable {

	private static final long serialVersionUID = -5362795894696157024L;

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Integer sourceTableNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long sourceRowNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Integer targetTableNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long targatRowNum;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Long spendTime;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private Integer waitingForSyecTableNums;
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private DataFlowProgressDetail.Status status;

	public Overview() {
		this.sourceTableNum = 0;
		this.sourceRowNum = -1L;
		this.targetTableNum = 0;
		this.targatRowNum = 0L;
	}

	public Integer getSourceTableNum() {
		return sourceTableNum;
	}

	public void setSourceTableNum(Integer sourceTableNum) {
		this.sourceTableNum = sourceTableNum;
	}

	public Long getSourceRowNum() {
		return sourceRowNum;
	}

	public void setSourceRowNum(Long sourceRowNum) {
		this.sourceRowNum = sourceRowNum;
	}

	public Integer getTargetTableNum() {
		return targetTableNum;
	}

	public void setTargetTableNum(Integer targetTableNum) {
		this.targetTableNum = targetTableNum;
	}

	public Long getTargatRowNum() {
		return targatRowNum;
	}

	public void setTargatRowNum(Long targatRowNum) {
		this.targatRowNum = targatRowNum;
	}

	public Long getSpendTime() {
		return spendTime;
	}

	public void setSpendTime(Long spendTime) {
		this.spendTime = spendTime;
	}

	public Integer getWaitingForSyecTableNums() {
		return waitingForSyecTableNums;
	}

	public void setWaitingForSyecTableNums(Integer waitingForSyecTableNums) {
		this.waitingForSyecTableNums = waitingForSyecTableNums;
	}

	public DataFlowProgressDetail.Status getStatus() {
		return status;
	}

	public void setStatus(DataFlowProgressDetail.Status status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "Overview{" +
				"sourceTableNum=" + sourceTableNum +
				", sourceRowNum=" + sourceRowNum +
				", targetTableNum=" + targetTableNum +
				", targatRowNum=" + targatRowNum +
				", spendTime=" + spendTime +
				", waitingForSyecTableNums=" + waitingForSyecTableNums +
				'}';
	}
}

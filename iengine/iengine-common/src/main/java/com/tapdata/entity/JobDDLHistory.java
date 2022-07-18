package com.tapdata.entity;

import java.io.Serializable;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2020-12-03 14:31
 **/
public class JobDDLHistory implements Serializable {

	private static final long serialVersionUID = 6294652927018180682L;

	private String dataFlowId;

	private String jobId;

	private String sourceConnId;

	/**
	 * {server: "5fc8a1f67c109033c567e58e"}
	 */
	private Map<String, String> source;

	/**
	 * {file: "mysql-bin.000004", pos: 365256841, snapshot: true}
	 */
	private Map<String, Object> position;

	private String ddl;

	private String line;

	public JobDDLHistory() {
	}

	public JobDDLHistory(String dataFlowId, String jobId, String sourceConnId, Map<String, String> source, Map<String, Object> position, String ddl, String line) {
		this.dataFlowId = dataFlowId;
		this.jobId = jobId;
		this.sourceConnId = sourceConnId;
		this.source = source;
		this.position = position;
		this.ddl = ddl;
		this.line = line;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getSourceConnId() {
		return sourceConnId;
	}

	public void setSourceConnId(String sourceConnId) {
		this.sourceConnId = sourceConnId;
	}

	public Map<String, String> getSource() {
		return source;
	}

	public void setSource(Map<String, String> source) {
		this.source = source;
	}

	public Map<String, Object> getPosition() {
		return position;
	}

	public void setPosition(Map<String, Object> position) {
		this.position = position;
	}

	public String getDdl() {
		return ddl;
	}

	public void setDdl(String ddl) {
		this.ddl = ddl;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	@Override
	public String toString() {
		return "JobDDLHistory{" +
				"dataflowId='" + dataFlowId + '\'' +
				", jobId='" + jobId + '\'' +
				", sourceConnId='" + sourceConnId + '\'' +
				", source=" + source +
				", position=" + position +
				", ddl='" + ddl + '\'' +
				", line='" + line + '\'' +
				'}';
	}
}

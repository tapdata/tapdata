package com.tapdata.entity.Message;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-11-03 12:42
 **/
public class JobDdlMessage extends Message implements Serializable {

	private static final long serialVersionUID = 7797260281628460557L;

	public static final String MSG = "JobDDL";
	public static final String TITLE = "JobDDL";

	/**
	 * job ddl
	 */
	private String jobName;

	private String source;

	private String target;

	private long scn;

	private String sql;

	private long timestamp;

	private String xid;

	private String mappingTemplate;

	private String sourceName;

	private String databaseName;

	private String schemaName;

	public JobDdlMessage(String level, String system, String msg, String title, String userId,
						 String email, String serverName, String sourceId, String jobName, String source, String target,
						 long scn, String sql, long timestamp, String xid, String mappingTemplate, String sourceName,
						 String databaseName, String schemaName) {
		super(level, system, msg, title, userId, email, serverName, sourceId);
		if (StringUtils.isAnyBlank(jobName, source, target, sql, xid, mappingTemplate, sourceName, databaseName)) {
			throw new IllegalArgumentException("Input params: jobName, source, target, sql, xid, mappingTemplate, sourceName, databaseName cannot be empty");
		}
		this.jobName = jobName;
		this.source = source;
		this.target = target;
		this.scn = scn;
		this.sql = sql;
		this.timestamp = timestamp;
		this.xid = xid;
		this.mappingTemplate = mappingTemplate;
		this.sourceName = sourceName;
		this.databaseName = databaseName;
		this.schemaName = schemaName;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public long getScn() {
		return scn;
	}

	public void setScn(long scn) {
		this.scn = scn;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getXid() {
		return xid;
	}

	public void setXid(String xid) {
		this.xid = xid;
	}

	public String getMappingTemplate() {
		return mappingTemplate;
	}

	public void setMappingTemplate(String mappingTemplate) {
		this.mappingTemplate = mappingTemplate;
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	@Override
	public String toString() {
		return "JobDdlMessage{" +
				"jobName='" + jobName + '\'' +
				", source='" + source + '\'' +
				", target='" + target + '\'' +
				", scn=" + scn +
				", sql='" + sql + '\'' +
				", timestamp=" + timestamp +
				", xid='" + xid + '\'' +
				", mappingTemplate='" + mappingTemplate + '\'' +
				", sourceName='" + sourceName + '\'' +
				", databaseName='" + databaseName + '\'' +
				", schemaName='" + schemaName + '\'' +
				'}';
	}
}

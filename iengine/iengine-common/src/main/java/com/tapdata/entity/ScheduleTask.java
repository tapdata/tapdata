package com.tapdata.entity;

import java.util.Date;
import java.util.Map;

public class ScheduleTask {

	public static final String TASK_TYPE_JS_FUNC = "JS_FUNC";
	public static final String TASK_TYPE_JAVA_FUNC = "JAVA_FUNC";

	private String id;

	private String task_name;

	private String cron_expression;

	private long period;

	private String status;

	private String create_user;

	private Date update_time;

	private String task_type;

	private Map<String, Object> task_data;

	private String agent_id;

	private long ping_time;

	private Object statsOffset;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTask_name() {
		return task_name;
	}

	public void setTask_name(String task_name) {
		this.task_name = task_name;
	}

	public String getCron_expression() {
		return cron_expression;
	}

	public void setCron_expression(String cron_expression) {
		this.cron_expression = cron_expression;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getCreate_user() {
		return create_user;
	}

	public void setCreate_user(String create_user) {
		this.create_user = create_user;
	}

	public Date getUpdate_time() {
		return update_time;
	}

	public void setUpdate_time(Date update_time) {
		this.update_time = update_time;
	}

	public String getTask_type() {
		return task_type;
	}

	public void setTask_type(String task_type) {
		this.task_type = task_type;
	}

	public String getAgent_id() {
		return agent_id;
	}

	public void setAgent_id(String agent_id) {
		this.agent_id = agent_id;
	}

	public long getPing_time() {
		return ping_time;
	}

	public void setPing_time(long ping_time) {
		this.ping_time = ping_time;
	}

	public Map<String, Object> getTask_data() {
		return task_data;
	}

	public void setTask_data(Map<String, Object> task_data) {
		this.task_data = task_data;
	}

	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}

	public Object getStatsOffset() {
		return statsOffset;
	}

	public void setStatsOffset(Object statsOffset) {
		this.statsOffset = statsOffset;
	}
}

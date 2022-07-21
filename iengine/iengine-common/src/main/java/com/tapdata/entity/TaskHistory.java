package com.tapdata.entity;

import java.util.Date;

public class TaskHistory {

	private String task_id;
	private String task_name;

	private int task_result_code;

	private Object task_result;

	private long task_duration;

	private Date task_start_time;

	private String agent_id;

	public TaskHistory() {
	}

	public TaskHistory(String task_id, String task_name, int task_result_code, String task_result, int task_duration, Date task_start_time, String agent_id) {
		this.task_id = task_id;
		this.task_result_code = task_result_code;
		this.task_name = task_name;
		this.task_result = task_result;
		this.task_duration = task_duration;
		this.task_start_time = task_start_time;
		this.agent_id = agent_id;
	}

	public String getTask_id() {
		return task_id;
	}

	public void setTask_id(String task_id) {
		this.task_id = task_id;
	}

	public int getTask_result_code() {
		return task_result_code;
	}

	public void setTask_result_code(int task_result_code) {
		this.task_result_code = task_result_code;
	}

	public Object getTask_result() {
		return task_result;
	}

	public void setTask_result(Object task_result) {
		this.task_result = task_result;
	}

	public long getTask_duration() {
		return task_duration;
	}

	public void setTask_duration(long task_duration) {
		this.task_duration = task_duration;
	}

	public Date getTask_start_time() {
		return task_start_time;
	}

	public void setTask_start_time(Date task_start_time) {
		this.task_start_time = task_start_time;
	}

	public String getAgent_id() {
		return agent_id;
	}

	public void setAgent_id(String agent_id) {
		this.agent_id = agent_id;
	}

	public String getTask_name() {
		return task_name;
	}

	public void setTask_name(String task_name) {
		this.task_name = task_name;
	}
}

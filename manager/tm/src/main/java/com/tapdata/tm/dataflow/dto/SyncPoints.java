/**
 * @title: SyncPoints
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class SyncPoints {

	private String connectionId;

	private String type;

	private String date;

	private String time;

	private String timezone;

	private String name;

	public String getConnectionId() {
		return connectionId;
	}

	public String getType() {
		return type;
	}

	public String getDate() {
		return date;
	}

	public String getTime() {
		return time;
	}

	public String getTimezone() {
		return timezone;
	}

	public String getName() {
		return name;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}

	public void setName(String name) {
		this.name = name;
	}
}

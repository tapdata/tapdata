/**
 * @title: SyncPoint
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

public class SyncPoint {

	private String type;

	private String date;

	private String timezone;

	public String getType() {
		return type;
	}

	public String getDate() {
		return date;
	}

	public String getTimezone() {
		return timezone;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}
}

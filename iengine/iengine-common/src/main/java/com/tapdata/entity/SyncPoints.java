/**
 * @title: SyncPoints
 * @description:
 * @author lk
 * @date 2020/6/17
 */
package com.tapdata.entity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

public class SyncPoints implements Serializable {

	private static final long serialVersionUID = -1062119734942870752L;
	private static Logger logger = LogManager.getLogger(SyncPoints.class);

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

	public void setTimezone(String timeZone) {
		this.timezone = timeZone;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

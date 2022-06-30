package com.tapdata.entity.dataflow;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-07-24 16:39
 **/
public class SyncPoint implements Serializable {

	private static final long serialVersionUID = 3358427835909396050L;
	private String type;
	private String date;
	private String timezone;

	public SyncPoint() {

	}

	public SyncPoint(String type, String date, String timezone) {
		this.type = type;
		this.date = date;
		this.timezone = timezone;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getTimezone() {
		return timezone;
	}

	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}

	public boolean isEmpty() {
		return StringUtils.isBlank(type);
	}

	@Override
	public String toString() {
		return "SyncPoint{" +
				"type='" + type + '\'' +
				", date='" + date + '\'' +
				", timezone='" + timezone + '\'' +
				'}';
	}
}

package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-07-29 23:38
 **/
public class Ttl implements Serializable {
	private static final long serialVersionUID = -349064165996762201L;
	private String fieldname;

	private Integer expire_minutes;

	public Ttl() {

	}

	public Ttl(String fieldname, Integer expire_minutes) {
		this.fieldname = fieldname;
		this.expire_minutes = expire_minutes;
	}

	public String getFieldname() {
		return fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	public Integer getExpire_minutes() {
		return expire_minutes;
	}

	public void setExpire_minutes(Integer expire_minutes) {
		this.expire_minutes = expire_minutes;
	}

	public boolean isEmpty() {
		return StringUtils.isBlank(fieldname)
				|| expire_minutes == null
				|| expire_minutes <= 0;
	}
}

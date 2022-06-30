package com.tapdata.entity;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2020-12-03 20:54
 **/
public class ResponseCount implements Serializable {

	private static final long serialVersionUID = 4618575908546650236L;

	private long count;

	public ResponseCount() {
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "ResponseCount{" +
				"count=" + count +
				'}';
	}
}

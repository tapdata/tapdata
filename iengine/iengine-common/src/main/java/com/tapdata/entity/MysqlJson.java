package com.tapdata.entity;

import com.tapdata.constant.JSONUtil;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;

/**
 * MySQL JSON 类型映射
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/9 下午12:29
 * </pre>
 */
public class MysqlJson {

	private String data;

	public MysqlJson() {
	}

	public MysqlJson(String data) {
		this.data = data;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "" + data;
	}

	public Object toObject() {
		try {
			String jsonStr = getData().trim();
			if (jsonStr.startsWith("{") && jsonStr.endsWith("}")) {
				return JSONUtil.json2Map(jsonStr);
			} else if (jsonStr.startsWith("[") && jsonStr.endsWith("]")) {
				return JSONUtil.json2List(jsonStr, Object.class);
			}
			throw new RuntimeException("String not range in '{}' or '[]'");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || getClass() != o.getClass()) return false;

		MysqlJson mysqlJson = (MysqlJson) o;
		return mysqlJson.getData().equals(getData());
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(getData())
				.toHashCode();
	}
}

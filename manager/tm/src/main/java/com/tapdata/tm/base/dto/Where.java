package com.tapdata.tm.base.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/11 3:10 下午
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Where extends HashMap<String, Object> {

	public Where and(String prop, Object value) {
		this.put(prop, value);
		return this;
	}

	public static Where where(String prop, Object value) {
		return new Where().and(prop, value);
	}

}

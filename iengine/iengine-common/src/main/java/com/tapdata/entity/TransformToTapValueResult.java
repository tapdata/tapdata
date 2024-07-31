package com.tapdata.entity;

import java.io.Serializable;
import java.util.Set;

/**
 * @author samuel
 * @Description
 * @create 2024-07-06 18:26
 **/
public class TransformToTapValueResult implements Serializable {
	private Set<String> beforeTransformedToTapValueFieldNames;
	private Set<String> afterTransformedToTapValueFieldNames;

	private TransformToTapValueResult() {
	}

	public static TransformToTapValueResult create() {
		return new TransformToTapValueResult();
	}

	public TransformToTapValueResult beforeTransformedToTapValueFieldNames(Set<String> fieldNames) {
		this.beforeTransformedToTapValueFieldNames = fieldNames;
		return this;
	}

	public TransformToTapValueResult afterTransformedToTapValueFieldNames(Set<String> fieldNames) {
		this.afterTransformedToTapValueFieldNames = fieldNames;
		return this;
	}

	public Set<String> getBeforeTransformedToTapValueFieldNames() {
		return beforeTransformedToTapValueFieldNames;
	}

	public Set<String> getAfterTransformedToTapValueFieldNames() {
		return afterTransformedToTapValueFieldNames;
	}
}

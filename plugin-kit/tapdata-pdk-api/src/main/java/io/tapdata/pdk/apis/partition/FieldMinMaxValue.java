package io.tapdata.pdk.apis.partition;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;

import java.util.Date;

/**
 * @author aplomb
 */
public class FieldMinMaxValue {
	public static final String TYPE_NUMBER = "number"; // long, double, int, short, float, etc.
	public static final String TYPE_STRING = "string"; // String
	public static final String TYPE_BOOLEAN = "bool"; // Boolean
	public static final String TYPE_DATE = "date"; // Date
	private String fieldName;
	public FieldMinMaxValue fieldName(String fieldName) {
		this.fieldName = fieldName;
		return this;
	}
	private String type;
	public FieldMinMaxValue type(String type) {
		this.type = type;
		return this;
	}
	public FieldMinMaxValue detectType(Object value) {
		if(value instanceof Number) {
			type = TYPE_NUMBER;
		} else if(value instanceof String) {
			type = TYPE_STRING;
		} else if(value instanceof Boolean) {
			type = TYPE_BOOLEAN;
		} else {
			Object dateObj = AnyTimeToDateTime.toDateTime(value);
			if(dateObj != null) {
				type = TYPE_DATE;
			} else {
				type = value.getClass().getName();
			}
		}
		return this;
	}
	private Object min;
	public FieldMinMaxValue min(Object min) {
		this.min = min;
		return this;
	}
	private Object max;
	public FieldMinMaxValue max(Object max) {
		this.max = max;
		return this;
	}

	public static FieldMinMaxValue create() {
		return new FieldMinMaxValue();
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Object getMin() {
		return min;
	}

	public void setMin(Object min) {
		this.min = min;
	}

	public Object getMax() {
		return max;
	}

	public void setMax(Object max) {
		this.max = max;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
}

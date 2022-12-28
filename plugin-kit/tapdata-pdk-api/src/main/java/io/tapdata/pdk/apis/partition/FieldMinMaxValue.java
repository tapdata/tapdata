package io.tapdata.pdk.apis.partition;

import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;

import java.util.Date;

/**
 * @author aplomb
 */
public class FieldMinMaxValue {

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
		if(value == null)
			return this;
		type = TypeSplitterMap.detectType(value);
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

	public boolean isAvailable() {
		return min != null && max != null && fieldName != null;
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

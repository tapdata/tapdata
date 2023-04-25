package com.tapdata.entity.schema;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;

public class SchemaApplyResult {
	public static final String OP_TYPE_CREATE = "CREATE";
	public static final String OP_TYPE_REMOVE = "REMOVE";
	public static final String OP_TYPE_CONVERT = "CONVERT";

	public static final String OP_TYPE_SET_PK = "SET_PK";
	public static final String OP_TYPE_UN_SET_PK = "UN_SET_PK";

	public static final String OP_TYPE_ADD_INDEX = "ADD_INDEX";

	public static final String OP_TYPE_REMOVE_INDEX = "REMOVE_INDEX";


	private String op;
	private String fieldName;
	private TapField tapField;

	private TapIndex tapIndex;

	public SchemaApplyResult() {
	}

	public SchemaApplyResult(String op, TapIndex tapIndex) {
		this.op = op;
		this.tapIndex = tapIndex;
	}

	public SchemaApplyResult(String op, String fieldName) {
		this.op = op;
		this.fieldName = fieldName;
	}

	public SchemaApplyResult(String op, String fieldName, TapField tapField) {
		this.op = op;
		this.fieldName = fieldName;
		this.tapField = tapField;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public TapField getTapField() {
		return tapField;
	}

	public void setTapField(TapField tapField) {
		this.tapField = tapField;
	}

	public TapIndex getTapIndex() {
		return tapIndex;
	}

	public void setTapIndex(TapIndex tapIndex) {
		this.tapIndex = tapIndex;
	}
}

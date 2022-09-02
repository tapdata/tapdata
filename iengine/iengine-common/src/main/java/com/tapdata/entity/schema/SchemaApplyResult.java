package com.tapdata.entity.schema;

import io.tapdata.entity.schema.TapField;

public class SchemaApplyResult {
  public static final String OP_TYPE_CREATE = "CREATE";
  public static final String OP_TYPE_REMOVE = "REMOVE";
  public static final String OP_TYPE_CONVERT = "CONVERT";
  private String op;
  private String fieldName;
  private TapField tapField;

  public SchemaApplyResult() {
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
}

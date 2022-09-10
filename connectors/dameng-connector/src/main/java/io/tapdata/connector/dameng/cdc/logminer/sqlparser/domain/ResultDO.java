package io.tapdata.connector.dameng.cdc.logminer.sqlparser.domain;

import io.tapdata.connector.dameng.cdc.logminer.sqlparser.contant.Operate;

import java.util.HashMap;
import java.util.Map;

public class ResultDO {
  private Operate op;
  private String schema;
  private String tableName;
  private Map<String, Object> data;

  public ResultDO() {
    this(null, new HashMap<>());
  }

  public ResultDO(Operate op) {
    this(op, new HashMap<>());
  }

  public ResultDO(Operate op, Map<String, Object> data) {
    this.op = op;
    this.data = data;
  }

  public Operate getOp() {
    return op;
  }

  public void setOp(Operate op) {
    this.op = op;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public void setData(Map<String, Object> data) {
    this.data = data;
  }

  public void putData(String key, Object val) {
    this.data.put(key, val);
  }

  public void putIfAbsent(String key, Object val) {
    this.data.putIfAbsent(key, val);
  }

  @Override
  public String toString() {
    return "ResultDO{" +
      "op=" + op +
      ", db='" + schema + '\'' +
      ", tableName='" + tableName + '\'' +
      ", data=" + data +
      '}';
  }
}

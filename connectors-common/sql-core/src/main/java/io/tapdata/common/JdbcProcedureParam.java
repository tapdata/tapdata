package io.tapdata.common;

public class JdbcProcedureParam {

  private String name;
  private int index;
  private String type;
  private Object jdbcType;

  public JdbcProcedureParam(String name, int index, String type, Object jdbcType) {
    this.name = name;
    this.index = index;
    this.type = type;
    this.jdbcType = jdbcType;
  }

  public String getName() {
    return name;
  }

  public int getIndex() {
    return index;
  }

  public String getType() {
    return type;
  }

  public Object getJdbcType() {
    return jdbcType;
  }
}

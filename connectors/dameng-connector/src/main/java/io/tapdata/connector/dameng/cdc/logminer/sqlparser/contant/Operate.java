package io.tapdata.connector.dameng.cdc.logminer.sqlparser.contant;

public enum Operate {
  Insert, Delete, Update,
  ;

  public static Operate parse(String str) {
    return Operate.valueOf(str);
  }

}

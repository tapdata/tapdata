package io.tapdata.common.constant;

import java.util.HashMap;
import java.util.Map;

public enum JavaType {

  String("java.lang.String"),
  Short("java.lang.Short"),
  Integer("java.lang.Integer"),
  Long("java.lang.Long"),
  Float("java.lang.Float"),
  Double("java.lang.Double"),
  Byte("java.lang.Byte"),
  Bytes("java.lang.Byte[]"),
  Boolean("java.lang.Boolean"),
  Date("java.util.Date"),
  Time("java.sql.Time"),
  BigDecimal("java.math.BigDecimal"),
  Objects("java.lang.Object[]"),
  Array(),
  Map("java.util.Map"),
  Null("null"),
  Unrecognized,
  Object("java.lang.Object"),
  Unsupported,
  ;

  private String description;

  JavaType() {
  }

  JavaType(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return name();
  }

  private static Map<String, JavaType> map = new HashMap<>();

  static {
    JavaType[] values = JavaType.values();
    for (JavaType value : values) {
      map.put(value.description, value);
    }
  }

  public static JavaType fromDesc(String description) {
    return map.get(description);
  }
}

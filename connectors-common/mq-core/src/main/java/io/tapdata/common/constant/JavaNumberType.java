package io.tapdata.common.constant;

import java.math.BigDecimal;
import java.util.stream.Stream;

public enum JavaNumberType {

    LONG(JavaType.Long, 20, 0),
    INT(JavaType.Integer, 11, 0),
    SHORT(JavaType.Short, 6, 0),
    BYTE(JavaType.Byte, 4, 0),
    DOUBLE(JavaType.Double, 0, 16),
    FLOAT(JavaType.Float, 0, 7),
    BIG_DECIMAL(JavaType.BigDecimal, 0, -1),
    ;

    private final JavaType javaType;
    private final int scale;
    private final int precision;

    JavaNumberType(JavaType javaType, int scale, int precision) {
        this.javaType = javaType;
        this.scale = scale;
        this.precision = precision;
    }

    public static JavaNumberType of(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Long) {
            return JavaNumberType.LONG;
        }
        if (obj instanceof Integer) {
            return JavaNumberType.INT;
        }
        if (obj instanceof Short) {
            return JavaNumberType.SHORT;
        }
        if (obj instanceof Byte) {
            return JavaNumberType.BYTE;
        }
        if (obj instanceof BigDecimal) {
            return JavaNumberType.BIG_DECIMAL;
        }
        if (obj instanceof Double) {
            return JavaNumberType.DOUBLE;
        }
        if (obj instanceof Float) {
            return JavaNumberType.FLOAT;
        }
        return null;
    }

    public static JavaNumberType of(int scale, int precision) {
        return Stream.of(JavaNumberType.values()).filter(e -> e.getScale() == scale && e.getPrecision() == precision).findFirst().orElse(null);
    }

    public JavaType getJavaType() {
        return javaType;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }
}

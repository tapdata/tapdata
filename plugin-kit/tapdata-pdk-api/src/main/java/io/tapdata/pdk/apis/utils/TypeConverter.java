package io.tapdata.pdk.apis.utils;

import java.util.List;

public interface TypeConverter {
    Long toLong(Object value);
    Integer toInteger(Object value);
    Short toShort(Object value);
    Byte toByte(Object value);
    String toString(Object value);
    List<String> toStringArray(Object value);
    Double toDouble(Object value);
    Float toFloat(Object value);
    Boolean toBoolean(Object value);
}

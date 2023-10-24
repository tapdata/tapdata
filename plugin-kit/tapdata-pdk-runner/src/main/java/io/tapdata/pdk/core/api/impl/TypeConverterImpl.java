package io.tapdata.pdk.core.api.impl;

import io.tapdata.pdk.apis.utils.TypeConverter;
import io.tapdata.entity.annotations.Implementation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Implementation(TypeConverter.class)
public class TypeConverterImpl implements TypeConverter {
    @Override
    public Long toLong(Object value) {
        if(value == null)
            return null;
        if(value instanceof Number) {
            return ((Number) value).longValue();
        }
        if(value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }

    @Override
    public Integer toInteger(Object value) {
        if(value == null)
            return null;
        if(value instanceof Number) {
            return ((Number) value).intValue();
        }
        if(value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }

    @Override
    public Short toShort(Object value) {
        if(value == null)
            return null;
        if(value instanceof Number) {
            return ((Number) value).shortValue();
        }
        if(value instanceof String) {
            try {
                return Short.parseShort((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }

    @Override
    public Byte toByte(Object value) {
        if(value == null)
            return null;
        if(value instanceof Number) {
            return ((Number) value).byteValue();
        }
        if(value instanceof String) {
            try {
                return Byte.parseByte((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }

    @Override
    public String toString(Object value) {
        if(value == null)
            return null;
        if(value instanceof String) {
            return (String) value;
        }
        if(value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    @Override
    public List<String> toStringArray(Object value) {
        if(value == null)
            return null;
        List<String> strs = new ArrayList<>();
        if(value instanceof Collection) {
            for(Object obj : (Collection<?>) value) {
                String str = toString(obj);
                if(str != null)
                    strs.add(str);
            }
        }
        return strs;
    }

    @Override
    public Double toDouble(Object value) {
        if(value == null)
            return null;
        if(value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if(value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }

    @Override
    public Float toFloat(Object value) {
        if(value == null)
            return null;
        if(value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if(value instanceof String) {
            try {
                return Float.parseFloat((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }

    @Override
    public Boolean toBoolean(Object value) {
        if(value == null)
            return null;
        if(value instanceof Boolean) {
            return (Boolean) value;
        }
        if(value instanceof String) {
            try {
                return Boolean.parseBoolean((String) value);
            } catch(Throwable throwable) {
                return null;
            }
        }
        return null;
    }
}

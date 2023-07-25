package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.Utils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;

/**
 * @author GavinXiao
 * @description SybaseDataTypeConvert create by Gavin
 * @create 2023/7/15 18:31
 **/
public interface SybaseDataTypeConvert {
    public static final String INSERT = "I";
    public static final String UPDATE = "U";
    public static final String DELETE = "D";

    public Object convert(Object fromValue, String sybaseType, ConnectionConfig config, NodeConfig nodeConfig);

    public default String objToString(Object obj, ConnectionConfig config, NodeConfig nodeConfig) {
        if (null == obj) return null;
        if (obj instanceof String) {
            String fromStr = (String) obj;
            try {
//                return new String(Utils.convertString(fromStr, config.getEncode(), config.getDecode()).getBytes(config.getDecode()), nodeConfig.getOutDecode());
                return nodeConfig.isAutoEncode() ? Utils.convertString(fromStr, nodeConfig.getEncode(), nodeConfig.getDecode()) : fromStr;
            } catch (Exception e) {
                return fromStr;
            }
        }
        if (obj instanceof Number || obj instanceof Boolean) {
            return "" + obj;
        } else {
            return toJson(obj);
        }
    }

    public default Date objToDateTime(Object obj, String format, String type) throws Exception {
        if (null == obj) return null;
        if (obj instanceof String) {
            return Utils.dateFormat((String) obj, format);
        } else if (obj instanceof Number) {
            try {
                return Utils.dateFormat(((Number) obj).intValue() + "", format);
            } catch (Exception e) {
                try {
                    return new Date(((Number) obj).longValue());
                } catch (Exception e1) {
                    throw new IllegalArgumentException("Error convert to " + type);
                }
            }
        } else {
            throw new IllegalArgumentException("Error convert to " + type);
        }
    }

    public default Boolean objToBoolean(Object obj) {
        if (null == obj) return false;
        if (obj instanceof Boolean) return (Boolean) obj;
        if (obj instanceof String) {
            try {
                return Boolean.parseBoolean((String) obj);
            } catch (Exception e) {
                throw new IllegalArgumentException("can not convert value to boolean, value: " + toJson(obj));
            }
        } else {
            throw new IllegalArgumentException("can not convert value to boolean, value: " + toJson(obj));
        }
    }

    public default BigDecimal objToNumber(Object obj) {
        if (null == obj) return null;
        if (obj instanceof String) {
            try {
                return new BigDecimal((String) obj);
            } catch (Exception e) {
                throw new IllegalArgumentException("can not convert value to boolean, value: " + toJson(obj));
            }
        } else if (obj instanceof Number) {
            return new BigDecimal(String.valueOf(obj));
        } else {
            throw new IllegalArgumentException("can not convert value to boolean, value: " + toJson(obj));
        }
    }

    public default Map<String, Object> objToMap(Object obj) {
        if (null == obj) return null;
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        } else if (obj instanceof String) {
            try {
                Object convertObj = fromJson((String) obj);
                if (convertObj instanceof Map) {
                    return (Map<String, Object>) convertObj;
                } else {
                    throw new IllegalArgumentException("can not convert value to json map, value: " + toJson(obj));
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("can not convert value to json map, value: " + toJson(obj));
            }
        } else {
            throw new IllegalArgumentException("can not convert value to json map, value: " + toJson(obj));
        }
    }

    public default Collection<Object> objToCollection(Object obj) {
        if (null == obj) return null;
        if (obj instanceof Collection) {
            return (Collection<Object>) obj;
        } else if (obj instanceof String) {
            try {
                Object convertObj = fromJson((String) obj);
                if (convertObj instanceof Collection) {
                    return (Collection<Object>) convertObj;
                } else {
                    throw new IllegalArgumentException("can not convert value to json Array, value: " + obj);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("can not convert value to json Array, value: " + obj);
            }
        } else if (obj.getClass().isArray()) {
            return new ArrayList<>(Arrays.asList((Object[]) obj));
        } else {
            throw new IllegalArgumentException("can not convert value to json Array, value: " + toJson(obj));
        }
    }

    public default Object objToBinary(Object obj) {
        if (null == obj) return null;
        if (obj instanceof String) {
            String binaryObj = (String) obj;
            return "0x" + binaryObj;
        }
        if (obj instanceof Map || obj instanceof Collection || obj.getClass().isArray()) {
            return toJson(obj).getBytes(StandardCharsets.UTF_8);
        } else {
            return obj.toString().getBytes(StandardCharsets.UTF_8);
        }
    }
}

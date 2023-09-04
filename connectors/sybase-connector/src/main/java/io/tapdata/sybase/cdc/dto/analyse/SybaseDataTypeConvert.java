package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.Utils;

import sun.misc.BASE64Decoder;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
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
    public static final String TAG = SybaseDataTypeConvert.class.getSimpleName();

    public static final String INSERT = "I";
    public static final String UPDATE = "U";
    public static final String DELETE = "D";

    public Object convert(Object fromValue, String sybaseType, final int typeNum, ConnectionConfig config, NodeConfig nodeConfig);

    public static String objToString(Object obj, ConnectionConfig config, NodeConfig nodeConfig) {
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

    public static Object objToTimestamp(Object obj, String type) throws Exception {
        if (null == obj) return null;
        if (obj instanceof String) {
            return Timestamp.valueOf((String)obj);
        } else if (obj instanceof Number) {
            try {
                return new Timestamp(((Number) obj).longValue());
            } catch (Exception e) {
                throw new IllegalArgumentException("Error convert to " + type);
            }
        } else {
            throw new IllegalArgumentException("Error convert to " + type);
        }
    }
    public static Object objToDateTime(Object obj, String format, String type) throws Exception {
        if (null == obj) return null;
        if (obj instanceof String) {
            return Utils.dateFormat((String) obj, format);
        } else if (obj instanceof Number) {
            try {
                return new Timestamp(((Number) obj).longValue());
            } catch (Exception e) {
                throw new IllegalArgumentException("Error convert to " + type);
            }
        } else {
            throw new IllegalArgumentException("Error convert to " + type);
        }
    }

    public static Boolean objToBoolean(Object obj) {
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

    public static BigDecimal objToNumber(Object obj) {
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

    public static Map<String, Object> objToMap(Object obj) {
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

    public static Collection<Object> objToCollection(Object obj) {
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

    public static Object objToBinary(Object obj) {
//        TapLogger.warn(TAG, "An BINARY data type not support in cdc now");
//        return null;
        if (null == obj) return null;
        if (obj instanceof String) {
            try {
                return obj;
//                String binaryObj = (String) obj;
//                BASE64Decoder decoder = new BASE64Decoder();
//                return decoder.decodeBuffer(binaryObj);
            } catch (Exception e) {
                return null;
            }
        }
        if (obj instanceof Map || obj instanceof Collection || obj.getClass().isArray()) {
            return toJson(obj).getBytes(StandardCharsets.UTF_8);
        } else {
            return obj.toString().getBytes(StandardCharsets.UTF_8);
        }
    }
}

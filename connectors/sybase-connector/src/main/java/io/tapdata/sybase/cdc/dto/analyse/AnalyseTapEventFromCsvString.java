package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.sybase.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;
import static io.tapdata.entity.schema.type.TapType.TYPE_ARRAY;
import static io.tapdata.entity.schema.type.TapType.TYPE_BINARY;
import static io.tapdata.entity.schema.type.TapType.TYPE_BOOLEAN;
import static io.tapdata.entity.schema.type.TapType.TYPE_DATE;
import static io.tapdata.entity.schema.type.TapType.TYPE_DATETIME;
import static io.tapdata.entity.schema.type.TapType.TYPE_MAP;
import static io.tapdata.entity.schema.type.TapType.TYPE_NUMBER;
import static io.tapdata.entity.schema.type.TapType.TYPE_RAW;
import static io.tapdata.entity.schema.type.TapType.TYPE_STRING;
import static io.tapdata.entity.schema.type.TapType.TYPE_TIME;
import static io.tapdata.entity.schema.type.TapType.TYPE_YEAR;

/**
 * @author GavinXiao
 * @description AnalyseTapEventFromCsvString create by Gavin
 * @create 2023/7/14 10:00
 **/
public class AnalyseTapEventFromCsvString implements AnalyseRecord<List<String>, TapRecordEvent> {
    @Override
    public TapRecordEvent analyse(List<String> record, TapTable tapTable) {
        // 6,NULL,1,
        // 2023-07-13 20:43:05.0,NULL,1,
        // "sfas"",""dsafas",NULL,1,
        // 8.9,NULL,1,
        // 2023-07-13 20:43:23.0,NULL,1
        // ,4,NULL,1,
        // I,"{""extractorId"":0,""transactionLogPageNumber"":901,""transactionLogRowNumber"":145,""operationLogPageNumber"":901,""operationLogRowNumber"":146,""catalogName"":""testdb"",""timestamp"":1689252221014,""extractionTimestamp"":1689252221015,""v"":0}","{""insertCount"":4,""updateCount"":0,""deleteCount"":0,""replaceCount"":0}"
        final int recordKeyCount = record.size();
        final int group = recordKeyCount / 3;
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        final int fieldsCount = nameFieldMap.size();

        final String cdcType = fieldsCount > group ? record.get((group - 1) * 3) : INSERT;
        String cdcInfoStr = fieldsCount > group ? record.get((group - 1) * 3 + 1) : null;
        Map<String, Object> cdcInfo = null;
        try {
            cdcInfo = (Map<String, Object>) fromJson(cdcInfoStr);
        } catch (Exception e) {
            cdcInfo = new HashMap<>();
        }
        if (null == cdcInfo) cdcInfo = new HashMap<>();


        int index = 0;
        String tableId = tapTable.getId();
        Map<String, Object> eventValue = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        for (Map.Entry<String, TapField> fieldEntry : nameFieldMap.entrySet()) {
            final String fieldName = fieldEntry.getKey();
            final TapField tapField = fieldEntry.getValue();
            if (!DELETE.equals(cdcType)) {
                int fieldValueIndex = index * 3;
                final Object value = recordKeyCount <= fieldValueIndex ? null : record.get(fieldValueIndex);
                eventValue.put(fieldName, convertToSybaseData(value, tapField));
            }
            if (!INSERT.equals(cdcType)) {
                int fieldBeforeValueIndex = index * 3 + 1;
                final Object beforeValue = recordKeyCount <= fieldBeforeValueIndex ? null : record.get(fieldBeforeValueIndex);
                before.put(fieldName, convertToSybaseData(beforeValue, tapField));
            }
            index++;
        }
        Object timestamp = cdcInfo.get("timestamp");
        long cdcReference = System.currentTimeMillis();
        try {
            cdcReference = Long.parseLong((String) timestamp);
        } catch (Exception ignore) {
        }
        switch (cdcType) {
            case INSERT:
                return TapSimplify.insertRecordEvent(eventValue, tableId).referenceTime(cdcReference);
            case DELETE:
                return TapSimplify.deleteDMLEvent(before, tableId).referenceTime(cdcReference);
            default:
                return TapSimplify.updateDMLEvent(before, eventValue, tableId).referenceTime(cdcReference);
        }
    }

    public static final String INSERT = "I";
    public static final String UPDATE = "U";
    public static final String DELETE = "D";

    public static Object convertToSybaseData(Object fromValue, TapField tapField) {
        if (null == fromValue || null == tapField) return null;
        if (fromValue instanceof String && "NULL".equals(fromValue)) return null;
        TapType tapType = tapField.getTapType();
        byte type = tapType.getType();
        switch (type) {
            case TYPE_STRING:
                return objToString(fromValue);
            case TYPE_YEAR:
                try {
                    return objToDateTime(fromValue, "yyyy", "year");
                } catch (Exception e) {
                    throw new CoreException("Can not convert value {} to year value", fromValue);
                }
            case TYPE_DATE:
                try {
                    return objToDateTime(fromValue, "yyyy-MM-dd", "date");
                } catch (Exception e) {
                    throw new CoreException("Can not convert value {} to year value", fromValue);
                }
            case TYPE_TIME:
                try {
                    return objToDateTime(fromValue, "HH:mm:ss", "date");
                } catch (Exception e) {
                    throw new CoreException("Can not convert value {} to year value", fromValue);
                }
            case TYPE_DATETIME:
                try {
                    return objToDateTime(fromValue, "yyyy-MM-dd HH:mm:ss.SSS", "date");
                } catch (Exception e) {
                    throw new CoreException("Can not convert value {} to year value", fromValue);
                }
            case TYPE_BOOLEAN:
                return objToBoolean(fromValue);
            case TYPE_NUMBER:
                return objToNumber(fromValue);
            case TYPE_BINARY:
                return objToBinary(fromValue);
            case TYPE_MAP:
                return objToMap(fromValue);
            case TYPE_ARRAY:
                return objToCollection(fromValue);
            case TYPE_RAW:
            default:
                return null;
        }
    }

    public static String objToString(Object obj) {
        if (null == obj) return null;
        if (obj instanceof String) return (String) obj;
        if (obj instanceof Number || obj instanceof Boolean) {
            return "" + obj;
        } else {
            return toJson(obj);
        }
    }

    public static Date objToDateTime(Object obj, String format, String type) throws Exception {
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

    public static Number objToNumber(Object obj) {
        if (null == obj) return null;
        if (obj instanceof String) {
            try {
                return 0;
            } catch (Exception e) {
                throw new IllegalArgumentException("can not convert value to boolean, value: " + toJson(obj));
            }
        } else if (obj instanceof Number) {

        } else {
            throw new IllegalArgumentException("can not convert value to boolean, value: " + toJson(obj));
        }
        return 0;
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

        return null;
    }
}

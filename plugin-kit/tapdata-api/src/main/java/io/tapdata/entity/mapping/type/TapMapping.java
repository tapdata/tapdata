package io.tapdata.entity.mapping.type;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.DataMap;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TapMapping {
    public static final String FIELD_TYPE_MAPPING = "_tapMapping";
    private static final String TAG = TapMapping.class.getSimpleName();

    public static final BigDecimal MIN_SCORE = BigDecimal.valueOf(10).pow(10000).negate();
    public static final BigDecimal MAX_SCORE = BigDecimal.valueOf(10).pow(10000);

    protected String name;
    protected String to;
    protected Boolean queryOnly = false;
    protected Integer priority = Integer.MAX_VALUE;
    protected Boolean pkEnablement = true;
    private static final Map<String, Class<?>> classCacheMap = new ConcurrentHashMap<>();

    public TapMapping() {

    }

    protected String getParam(Map<String, String> params, String key) {
        if(params != null)
            return params.get(key);
        return null;
    }

    protected Object getObject(Map<String, Object> map, String key) {
        if(map != null)
            return map.get(key);
        return null;
    }


    /**
     * Handle the value of open type json.
     *
     * for example, the value of below,
     * "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": true, "to": "TapNumber"}
     *
     * @param info
     */
    public abstract void from(Map<String, Object> info);

    public static TapMapping build(DataMap info) {
        return TapMapping.build(((Map<String, Object>)info));
    }
    public static TapMapping build(Map<String, Object> info) {
        String to = null;
        Object toObj = info.get("to");
        if(toObj instanceof String)
            to = (String) toObj;

        String name = null;
        Object nameObj = info.get("name");
        if(nameObj instanceof String)
            name = (String) nameObj;

        Boolean queryOnly = null;
        Object queryOnlyObj = info.get("queryOnly");
        if(queryOnlyObj instanceof Boolean)
            queryOnly = (Boolean) queryOnlyObj;

        Boolean pkEnablement = null;
        Object pkEnablementObj = info.get("pkEnablement");
        if(pkEnablementObj instanceof Boolean)
            pkEnablement = (Boolean) pkEnablementObj;

        Integer priority = null;
        Object priorityObj = info.get("priority");
        if(priorityObj instanceof Integer)
            priority = (Integer) priorityObj;

        if(to == null)
            return null;

        String typeMappingClass = TapMapping.class.getPackage().getName() + "." + to + "Mapping";
        Class<?> mappingClass = classCacheMap.get(typeMappingClass);
        if(mappingClass == null) {
            synchronized (classCacheMap) {
                mappingClass = classCacheMap.get(typeMappingClass);
                if(mappingClass == null) {
                    try {
                        mappingClass = Class.forName(typeMappingClass);
                        if(!TapMapping.class.isAssignableFrom(mappingClass)) {
                            return null;
                        }
                        classCacheMap.put(typeMappingClass, mappingClass);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }
        }

        try {
            TapMapping tapMapping = (TapMapping) mappingClass.getConstructor().newInstance();
            tapMapping.to = to;
            tapMapping.name = name;
            if(priority != null)
                tapMapping.priority = priority;
            if(pkEnablement != null)
                tapMapping.pkEnablement = pkEnablement;
            tapMapping.queryOnly = queryOnly;
            tapMapping.from(info);
            return tapMapping;
        } catch (Throwable e) {
            e.printStackTrace();
            TapLogger.error(TAG, "TapMapping name {} to {} from info {} failed, {}", name, to, info, e.getMessage());
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public abstract TapType toTapType(String dataType, Map<String, String> params);

    public abstract TapResult<String> fromTapType(String typeExpression, TapType tapType);

    protected String removeBracketVariables(String typeExpression, int startPos) {
        int pos = typeExpression.indexOf("[", startPos);
        while (pos >= 0) {
            int endPos = typeExpression.indexOf("]", pos);
            int embeddedStartPos = typeExpression.indexOf("[", pos + 1);
            if (embeddedStartPos >= 0 && embeddedStartPos < endPos) {
                typeExpression = removeBracketVariables(typeExpression, embeddedStartPos);
            } else {
                if (endPos >= 0) {
                    typeExpression = typeExpression.substring(0, pos) + typeExpression.substring(endPos + 1);
                } else {
                    typeExpression = typeExpression.substring(0, pos) + typeExpression.substring(pos + 1);
                }
            }
            pos = typeExpression.indexOf("[", startPos);
        }
        return typeExpression;
    }

    protected String clearBrackets(String typeExpression, String str) {
        return clearBrackets(typeExpression, str, true);
    }
    protected String clearBrackets(String typeExpression, String str, boolean needSpace) {
        if(str == null)
            return typeExpression;
        int pos = typeExpression.indexOf(str);
        if (pos >= 0) {
            int startPos = typeExpression.lastIndexOf("[", pos);
            if (startPos >= 0) {
                int endPos = typeExpression.indexOf("]", pos);
                if (endPos >= 0) {
                    typeExpression = typeExpression.substring(0, startPos) + (needSpace ? " " : "") + typeExpression.substring(startPos + 1);

                    endPos = typeExpression.indexOf("]", pos);
                    typeExpression = typeExpression.substring(0, endPos) + typeExpression.substring(endPos + 1);
                }
            }
        }
        return typeExpression;
    }

    public Boolean getQueryOnly() {
        return queryOnly;
    }

    public void setQueryOnly(Boolean queryOnly) {
        this.queryOnly = queryOnly;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Boolean getPkEnablement() {
        return pkEnablement;
    }

    public void setPkEnablement(Boolean pkEnablement) {
        this.pkEnablement = pkEnablement;
    }

    /**
     * The bigger the best
     *
     * @param field
     * @return
     */
    public abstract BigDecimal matchingScore(TapField field);
}

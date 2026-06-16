package com.tapdata.tm.lineage.util;

import com.tapdata.tm.lineage.entity.LineageType;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 17:11 Create
 * @description
 */
public final class LineageTypeUtil {
    public static final LineageType DEFAULT_LINEAGE_TYPE = LineageType.ALL_STREAM;

    private LineageTypeUtil() {}


    public static LineageType initLineageType(String type) {
        return initLineageType(type, DEFAULT_LINEAGE_TYPE);
    }

    public static LineageType initLineageType(String type, LineageType defaultType) {
        if (StringUtils.isBlank(type)) {
            return defaultType;
        }
        LineageType lineageType = LineageType.fromType(type);
        if (null == lineageType) {
            lineageType = defaultType;
        }
        return lineageType;
    }
}

package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.entity.error.CoreException;

import java.math.BigDecimal;

/**
 * @author GavinXiao
 * @description DefaultConvert create by Gavin
 * @create 2023/7/15 18:36
 **/
public class DefaultConvert implements SybaseDataTypeConvert {
    public static final int CONVERT_ERROR_CODE = 362430;
    @Override
    public Object convert(Object fromValue,final String sybaseType) {
        if (null == fromValue || null == sybaseType) return null;
        if (fromValue instanceof String && "NULL".equals(fromValue)) return null;
        String type = sybaseType.toUpperCase();
        BigDecimal bigDecimal = null;
        try {
            switch (type) {
                case "DATE":
                    return objToDateTime(fromValue, "yyyy-MM-dd", "DATE");
                case "TIME":
                    return objToDateTime(fromValue, "HH:mm:ss", "TIME");
                case "BIGTIME":
                    return objToDateTime(fromValue, "HH:mm:ss.SSS", "BIGTIME");
                case "INT":
                case "TINYINT":
                    bigDecimal = objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.intValue();
                case "SMALLINT":
                    bigDecimal = objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.byteValue();
                case "BIGINT":
                    bigDecimal = objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.longValue();
                case "DOUBLE":
                    bigDecimal = objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.doubleValue();
                case "SMALLMONEY":
                case "MONEY":
                case "REAL":
                case "BIT":
                    bigDecimal = objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.floatValue();
                case "IMAGE":
                    return objToBinary(fromValue);
                default:
                    if (type.contains("DATETIME")
                            //|| "SMALLDATETIME".equals(type)
                            //|| "DATETIME".equals(type)
                            //|| "BIGDATETIME".equals(type)
                            || "TIMESTAMP".equals(type)) {
                        return objToDateTime(fromValue, "yyyy-MM-dd HH:mm:ss.SSS", type);
                    } else if (type.startsWith("FLOAT")) {
                        bigDecimal = objToNumber(fromValue);
                        return null == bigDecimal ? null : bigDecimal.floatValue();
                    } else if (type.startsWith("DECIMAL")) {
                        return objToNumber(fromValue);
                    } else if (type.startsWith("NUMERIC")) {
                        return objToNumber(fromValue);
                    } else if (type.contains("CHAR")
                            //|| type.startsWith("NCHAR")
                            //|| type.startsWith("UNICHAR")
                            //|| type.startsWith("VARCHAR")
                            //|| type.startsWith("NVARCHAR")
                            //|| type.startsWith("UNIVARCHAR")
                            || type.contains("TEXT")
                            //|| type.startsWith("UNITEXT")
                            || type.startsWith("SYSNAME")
                            || type.startsWith("LONGSYSNAME")) {
                        return objToString(fromValue);
                    } else if (type.contains("BINARY")
                            //|| type.startsWith("VARBINARY")
                    ) {
                        return objToBinary(fromValue);
                    } else {
                        throw new CoreException(CONVERT_ERROR_CODE,"Found a type that cannot be processed when cdc: {}", type);
                    }
            }
        } catch (Exception e) {
            if (e instanceof CoreException && ((CoreException)e).getCode() == CONVERT_ERROR_CODE){
                throw (CoreException)e;
            }
            throw new CoreException("Can not convert value {} to {} value, msg: {}", fromValue, type, e.getMessage());
        }
    }
}

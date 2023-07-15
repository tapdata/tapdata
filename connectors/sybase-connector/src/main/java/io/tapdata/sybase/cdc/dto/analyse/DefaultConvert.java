package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.entity.error.CoreException;

import java.math.BigDecimal;

/**
 * @author GavinXiao
 * @description DefaultConvert create by Gavin
 * @create 2023/7/15 18:36
 **/
public class DefaultConvert implements SybaseDataTypeConvert {
    @Override
    public Object convert(Object fromValue,final String sybaseType) {
        if (null == fromValue || null == sybaseType) return null;
        if (fromValue instanceof String && "NULL".equals(fromValue)) return null;
        String type = sybaseType.toUpperCase();
        if ("DATE".equals(type)){
            try {
                return objToDateTime(fromValue, "yyyy-MM-dd", "DATE");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("TIME".equals(type)){
            try {
                return objToDateTime(fromValue, "HH:mm:ss", "TIME");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("BIGTIME".equals(type)){
            try {
                return objToDateTime(fromValue, "HH:mm:ss.SSS", "BIGTIME");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("SMALLDATETIME".equals(type)){
            try {
                return objToDateTime(fromValue, "yyyy-MM-dd HH:mm:ss.SSS", "SMALLDATETIME");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("DATETIME".equals(type)){
            try {
                return objToDateTime(fromValue, "yyyy-MM-dd HH:mm:ss.SSS", "DATETIME");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("BIGDATETIME".equals(type)){
            try {
                return objToDateTime(fromValue, "yyyy-MM-dd HH:mm:ss.SSS", "BIGDATETIME");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("TIMESTAMP".equals(type)){
            try {
                return objToDateTime(fromValue, "yyyy-MM-dd HH:mm:ss.SSS", "TIMESTAMP");
            } catch (Exception e) {
                throw new CoreException("Can not convert value {} to year value", fromValue);
            }
        }
        else if ("INT".equals(type)) {
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.intValue();
        }
        else if ("SMALLINT".equals(type)){
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.byteValue();
        }
        else if ("TINYINT".equals(type)){
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.intValue();
        }
        else if (type.startsWith("FLOAT")) {
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.floatValue();
        }
        else if ("REAL".equals(type)) {
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.floatValue();
        }
        else if ("BIT".equals(type)){
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.byteValue();
        }
        else if ("BIGINT".equals(type)){
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.longValue();
        }
        else if (type.startsWith("DECIMAL")){
            return objToNumber(fromValue);
        }
        else if (type.startsWith("NUMERIC")){
            return objToNumber(fromValue);
        }
        else if ("SMALLMONEY".equals(type)) {
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.floatValue();
        }
        else if ("MONEY".equals(type)) {
            BigDecimal bigDecimal = objToNumber(fromValue);
            return null == bigDecimal ? null : bigDecimal.floatValue();
        }
        else if (type.contains("CHAR")
                //|| type.startsWith("NCHAR")
                //|| type.startsWith("UNICHAR")
                //|| type.startsWith("VARCHAR")
                //|| type.startsWith("NVARCHAR")
                //|| type.startsWith("UNIVARCHAR")
                || type.contains("TEXT")
                //|| type.startsWith("UNITEXT")
                || type.startsWith("SYSNAME")
                || type.startsWith("LONGSYSNAME") ) {
            return objToString(fromValue);
        }
        else if (type.startsWith("BINARY")){
            return objToBinary(fromValue);
        }
        else if (type.startsWith("VARBINARY")){
            return objToBinary(fromValue);
        }
        else if ("IMAGE".equals(type)){
            return objToBinary(fromValue);
        }
        else {
            throw new CoreException("Found a type that cannot be processed when cdc: {}", type);
        }
    }
}

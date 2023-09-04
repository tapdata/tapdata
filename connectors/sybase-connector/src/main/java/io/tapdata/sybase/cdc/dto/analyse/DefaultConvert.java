package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;

import java.math.BigDecimal;

/**
 * @author GavinXiao
 * @description DefaultConvert create by Gavin
 * @create 2023/7/15 18:36
 **/
public class DefaultConvert implements SybaseDataTypeConvert {
    public static final String TAG = SybaseDataTypeConvert.class.getSimpleName();
    public static final int CONVERT_ERROR_CODE = 362430;

    @Override
    public Object convert(Object fromValue, final String sybaseType, final int typeNum, ConnectionConfig config, NodeConfig nodeConfig) {
        if (null == fromValue || null == sybaseType) return null;
        if (fromValue instanceof String && "NULL".equals(fromValue)) return null;
        BigDecimal bigDecimal = null;

        String type = sybaseType.toUpperCase();
        try {
            switch (typeNum) {
                case TableTypeEntity.Type.CHAR:
                case TableTypeEntity.Type.TEXT:
                    return SybaseDataTypeConvert.objToString(fromValue, config, nodeConfig);
                case TableTypeEntity.Type.DATE:
                    return SybaseDataTypeConvert.objToDateTime(fromValue, "yyyy-MM-dd", "DATE");
                case TableTypeEntity.Type.TIME:
                    return SybaseDataTypeConvert.objToDateTime(fromValue, "HH:mm:ss", "TIME");
                case TableTypeEntity.Type.BIG_TIME:
                    return SybaseDataTypeConvert.objToDateTime(fromValue, "HH:mm:ss.SSS", "BIGTIME");
                case TableTypeEntity.Type.DATETIME:
                case TableTypeEntity.Type.SMALL_DATETIME:
                    return SybaseDataTypeConvert.objToTimestamp(fromValue, type);
                case TableTypeEntity.Type.INT:
                    bigDecimal = SybaseDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.intValue();
                case TableTypeEntity.Type.SMALLINT:
                    bigDecimal = SybaseDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.byteValue();
                case TableTypeEntity.Type.BIGINT:
                    bigDecimal = SybaseDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.longValue();
                case TableTypeEntity.Type.DOUBLE:
                    bigDecimal = SybaseDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.doubleValue();
                case TableTypeEntity.Type.BIT:
                case TableTypeEntity.Type.FLOAT:
                    bigDecimal = SybaseDataTypeConvert.objToNumber(fromValue);
                    return null == bigDecimal ? null : bigDecimal.floatValue();
                case TableTypeEntity.Type.IMAGE:
                case TableTypeEntity.Type.BINARY:
                    //TapLogger.warn(TAG, "An {} data type not support in cdc now", sybaseType);
                    return SybaseDataTypeConvert.objToBinary(fromValue);
                case TableTypeEntity.Type.DECIMAL:
                    return SybaseDataTypeConvert.objToNumber(fromValue);
                case TableTypeEntity.Type.NUMERIC:
                    return SybaseDataTypeConvert.objToNumber(fromValue);
                default:
                    throw new CoreException(CONVERT_ERROR_CODE, "Found a type that cannot be processed when cdc: {}", sybaseType);

            }
        } catch (Exception e) {
            if (e instanceof CoreException && ((CoreException) e).getCode() == CONVERT_ERROR_CODE) {
                throw (CoreException) e;
            }
            throw new CoreException("Can not convert value {} to {} value, msg: {}", fromValue, type, e.getMessage());
        }
    }

    private String dateTimeFormat(String datetimeType, final int defaultCount) {
        String formatStart = "yyyy-MM-dd HH:mm:ss";
        if (null == datetimeType || "".equals(datetimeType)) return formatStart;
        int sCount = defaultCount;
        if (datetimeType.matches(".*\\(\\d*\\).*")) {
            int index = datetimeType.lastIndexOf('(');
            int lIndex = datetimeType.lastIndexOf(')');
            if (index > 0 && lIndex > index) {
                try {
                    sCount = Integer.parseInt(datetimeType.substring(index, lIndex));
                } catch (Exception e) {

                }
            }
        }
        if (sCount < 0 && defaultCount > 0) {
            sCount = defaultCount;
        }
        if (sCount > 0) {
            StringBuilder format = new StringBuilder(formatStart);
            format.append(".");
            for (int i = 0; i < sCount; i++) {
                format.append("S");
            }
            return format.toString();
        }
        return formatStart;
    }
}

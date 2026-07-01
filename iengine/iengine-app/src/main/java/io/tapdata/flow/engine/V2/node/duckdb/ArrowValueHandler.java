package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;

/**
 * Arrow 和 DuckDB Appender 值处理工具类
 * 
 * 统一处理 Arrow Vector 和 DuckDB Appender 的值设置逻辑
 * 确保所有数据类型的一致性和正确性
 * 
 * 支持的数据类型：
 * 1. 整数类型：Byte, Short, Integer, Long, BigInteger
 * 2. 浮点类型：Float, Double, BigDecimal
 * 3. 布尔类型：Boolean
 * 4. 字符串类型：String
 * 5. 日期时间类型：Date, Timestamp, LocalDateTime, LocalDate, DateTime, TapDateTimeValue
 * 6. 二进制类型：byte[]
 * 7. 复合类型：Collection<?>, Map<?,?>
 * 8. 特殊类型：null, 未知类型（toString兜底）
 */
public class ArrowValueHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(ArrowValueHandler.class);
    
    private ArrowValueHandler() {
        // 工具类，不允许实例化
    }
    
    // ==================== Arrow Vector 值设置 ====================
    
    /**
     * 统一设置 Arrow Vector 的值
     * 
     * @param vector Arrow 向量
     * @param index 行索引
     * @param value 值
     */
    public static void setVectorValue(ValueVector vector, int index, Object value) {
        if (value == null) {
            setVectorNull(vector, index);
            return;
        }
        
        try {
            if (vector instanceof BigIntVector) {
                setBigIntVector((BigIntVector) vector, index, value);
            } else if (vector instanceof IntVector) {
                setIntVector((IntVector) vector, index, value);
            } else if (vector instanceof SmallIntVector) {
                setSmallIntVector((SmallIntVector) vector, index, value);
            } else if (vector instanceof TinyIntVector) {
                setTinyIntVector((TinyIntVector) vector, index, value);
            } else if (vector instanceof Float4Vector) {
                setFloat4Vector((Float4Vector) vector, index, value);
            } else if (vector instanceof Float8Vector) {
                setFloat8Vector((Float8Vector) vector, index, value);
            } else if (vector instanceof BitVector) {
                setBitVector((BitVector) vector, index, value);
            } else if (vector instanceof VarCharVector) {
                setVarCharVector((VarCharVector) vector, index, value);
            } else if (vector instanceof VarBinaryVector) {
                setVarBinaryVector((VarBinaryVector) vector, index, value);
            } else {
                logger.warn("Unsupported vector type: {}", vector.getClass().getName());
                setVectorNull(vector, index);
            }
        } catch (Exception e) {
            logger.warn("Failed to set value at index {}: {}", index, e.getMessage());
            setVectorNull(vector, index);
        }
    }
    
    /**
     * 设置 Vector 的 null 值
     */
    private static void setVectorNull(ValueVector vector, int index) {
        if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, 0);
            ((BigIntVector) vector).setNull(index);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, 0);
            ((IntVector) vector).setNull(index);
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setSafe(index, (short) 0);
            ((SmallIntVector) vector).setNull(index);
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setSafe(index, (byte) 0);
            ((TinyIntVector) vector).setNull(index);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, 0f);
            ((Float4Vector) vector).setNull(index);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, 0d);
            ((Float8Vector) vector).setNull(index);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, 0);
            ((BitVector) vector).setNull(index);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, new byte[0]);
            ((VarCharVector) vector).setNull(index);
        } else if (vector instanceof VarBinaryVector) {
            ((VarBinaryVector) vector).setSafe(index, new byte[0]);
            ((VarBinaryVector) vector).setNull(index);
        }
    }
    
    /**
     * 设置 BigIntVector 的值
     */
    private static void setBigIntVector(BigIntVector vector, int index, Object value) {
        if (value instanceof Long) {
            vector.setSafe(index, (Long) value);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).longValue());
        } else if (value instanceof BigInteger) {
            vector.setSafe(index, ((BigInteger) value).longValue());
        } else {
            vector.setSafe(index, Long.parseLong(value.toString()));
        }
    }
    
    /**
     * 设置 IntVector 的值
     */
    private static void setIntVector(IntVector vector, int index, Object value) {
        if (value instanceof Integer) {
            vector.setSafe(index, (Integer) value);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).intValue());
        } else {
            vector.setSafe(index, Integer.parseInt(value.toString()));
        }
    }
    
    /**
     * 设置 SmallIntVector 的值
     */
    private static void setSmallIntVector(SmallIntVector vector, int index, Object value) {
        if (value instanceof Short) {
            vector.setSafe(index, (Short) value);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).shortValue());
        } else {
            vector.setSafe(index, Short.parseShort(value.toString()));
        }
    }
    
    /**
     * 设置 TinyIntVector 的值
     */
    private static void setTinyIntVector(TinyIntVector vector, int index, Object value) {
        if (value instanceof Byte) {
            vector.setSafe(index, (Byte) value);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).byteValue());
        } else {
            vector.setSafe(index, Byte.parseByte(value.toString()));
        }
    }
    
    /**
     * 设置 Float4Vector 的值
     */
    private static void setFloat4Vector(Float4Vector vector, int index, Object value) {
        if (value instanceof Float) {
            vector.setSafe(index, (Float) value);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).floatValue());
        } else {
            vector.setSafe(index, Float.parseFloat(value.toString()));
        }
    }
    
    /**
     * 设置 Float8Vector 的值
     */
    private static void setFloat8Vector(Float8Vector vector, int index, Object value) {
        if (value instanceof Double) {
            vector.setSafe(index, (Double) value);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).doubleValue());
        } else {
            vector.setSafe(index, Double.parseDouble(value.toString()));
        }
    }
    
    /**
     * 设置 BitVector 的值
     */
    private static void setBitVector(BitVector vector, int index, Object value) {
        if (value instanceof Boolean) {
            vector.setSafe(index, (Boolean) value ? 1 : 0);
        } else if (value instanceof Number) {
            vector.setSafe(index, ((Number) value).intValue() != 0 ? 1 : 0);
        } else {
            vector.setSafe(index, Boolean.parseBoolean(value.toString()) ? 1 : 0);
        }
    }
    
    /**
     * 设置 VarCharVector 的值
     */
    private static void setVarCharVector(VarCharVector vector, int index, Object value) {
        String strValue = convertToString(value);
        vector.setSafe(index, strValue.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * 设置 VarBinaryVector 的值
     */
    private static void setVarBinaryVector(VarBinaryVector vector, int index, Object value) {
        if (value instanceof byte[]) {
            vector.setSafe(index, (byte[]) value);
        } else {
            vector.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
    
    /**
     * 将值转换为字符串（处理日期时间类型）
     */
    private static String convertToString(Object value) {
        if (value instanceof Timestamp) {
            return value.toString();
        } else if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime()).toString();
        } else if (value instanceof LocalDateTime) {
            return value.toString();
        } else if (value instanceof LocalDate) {
            return value.toString();
        } else if (value instanceof DateTime) {
            long epochMs = ((DateTime) value).toInstant().toEpochMilli();
            return new Timestamp(epochMs).toString();
        } else if (value instanceof TapDateTimeValue) {
            DateTime dt = ((TapDateTimeValue) value).getValue();
            if (dt != null) {
                long epochMs = dt.toInstant().toEpochMilli();
                return new Timestamp(epochMs).toString();
            }
            return value.toString();
        } else {
            return value.toString();
        }
    }
    
    // ==================== DuckDB Appender 值设置 ====================
    
    /**
     * 统一设置 DuckDB Appender 的值
     * 
     * @param appender DuckDB Appender
     * @param value 值
     * @throws Exception 可能的异常
     */
    public static void appendToAppender(org.duckdb.DuckDBAppender appender, Object value) throws Exception {
        if (value == null) {
            appender.append((String) null);
            return;
        }
        
        // 整数类型 - 统一转换为 long，避免重载决议问题
        if (value instanceof Integer) {
            appender.append(((Integer) value).longValue());
        } else if (value instanceof Long) {
            appender.append((Long) value);
        } else if (value instanceof Short) {
            appender.append(((Short) value).longValue());
        } else if (value instanceof Byte) {
            appender.append(((Byte) value).longValue());
        } else if (value instanceof BigInteger) {
            appender.append((BigInteger) value);
        } 
        // 浮点类型 - 统一转换为 double
        else if (value instanceof Float) {
            appender.append(((Float) value).doubleValue());
        } else if (value instanceof Double) {
            appender.append((Double) value);
        } else if (value instanceof BigDecimal) {
            appender.append((BigDecimal) value);
        } 
        // 布尔类型
        else if (value instanceof Boolean) {
            appender.append((Boolean) value);
        } 
        // 字符串类型
        else if (value instanceof String) {
            appender.append((String) value);
        } 
        // 日期时间类型
        else if (value instanceof Timestamp) {
            appender.append(((Timestamp) value).toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime());
        } else if (value instanceof java.util.Date) {
            appender.append(new Timestamp(((java.util.Date) value).getTime()));
        } else if (value instanceof LocalDateTime) {
            appender.append((LocalDateTime) value);
        } else if (value instanceof LocalDate) {
            appender.append((LocalDate) value);
        } else if (value instanceof DateTime) {
            appender.append(new Timestamp(((DateTime) value).toInstant().toEpochMilli()));
        } else if (value instanceof TapDateTimeValue) {
            DateTime dt = ((TapDateTimeValue) value).getValue();
            if (dt != null) {
                appender.append(new Timestamp(dt.toInstant().toEpochMilli()));
            } else {
                appender.append((String) null);
            }
        } 
        // 二进制类型
        else if (value instanceof byte[]) {
            appender.append((byte[]) value);
        } 
        // 复合类型
        else if (value instanceof Collection<?>) {
            appender.append((Collection<?>) value);
        } else if (value instanceof Map<?, ?>) {
            appender.append((Map<?, ?>) value);
        } 
        // 未知类型兜底
        else {
            appender.append(value.toString());
        }
    }
}

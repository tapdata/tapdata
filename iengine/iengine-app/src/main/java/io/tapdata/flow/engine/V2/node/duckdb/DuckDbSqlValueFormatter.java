package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * DuckDB SQL 字段值格式化工具类
 * 
 * <p>统一处理拼接 DuckDB SQL 时的字段值处理逻辑，避免各模块重复实现。</p>
 * 
 * <p>支持的类型：
 * <ul>
 *   <li>null → "NULL"</li>
 *   <li>String → "'...'" (自动转义单引号和其他特殊字符)</li>
 *   <li>Number (Integer, Long, Double, Float, BigDecimal, BigInteger) → 直接输出</li>
 *   <li>Boolean → "TRUE" / "FALSE"</li>
 *   <li>Date / Timestamp / LocalDateTime / LocalDate → 标准 SQL 时间格式</li>
 *   <li>byte[] → DuckDB blob 字面量格式 (x'...')</li>
 *   <li>Collection / Map → JSON 字符串</li>
 * </ul>
 * </p>
 * 
 * <p>注意事项：
 * <ul>
 *   <li>字符串中的单引号会被转义为两个单引号</li>
 *   <li>字符串中的反斜杠会被转义为两个反斜杠</li>
 *   <li>时间格式遵循 DuckDB 的标准格式</li>
 *   <li>二进制数据使用 DuckDB 的 blob 字面量格式</li>
 * </ul>
 * </p>
 */
public class DuckDbSqlValueFormatter {
    
    private static final Logger logger = LoggerFactory.getLogger(DuckDbSqlValueFormatter.class);
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter DATE_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    /**
     * 格式化值为 SQL 字面量
     * 
     * <p>此方法将 Java 对象转换为可在 DuckDB SQL 语句中使用的字面量表示。
     * 生成的字符串可以直接拼接到 SQL 语句中。</p>
     * 
     * @param value 字段值
     * @return SQL 字面量字符串，可直接拼接到 SQL 语句中
     */
    public static String format(Object value) {
        if (value == null) {
            return "NULL";
        }
        
        // String - 需要转义单引号和反斜杠
        if (value instanceof String) {
            return formatString((String) value);
        }
        
        // Number - 直接输出
        if (value instanceof Number) {
            return formatNumber((Number) value);
        }
        
        // Boolean - DuckDB 使用 TRUE/FALSE
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        
        // 时间类型处理
        if (value instanceof Timestamp) {
            return formatTimestamp((Timestamp) value);
        }
        
        if (value instanceof Date) {
            return formatDate((Date) value);
        }
        
        if (value instanceof LocalDateTime) {
            return formatLocalDateTime((LocalDateTime) value);
        }
        
        if (value instanceof LocalDate) {
            return formatLocalDate((LocalDate) value);
        }
        
        // Tapdata DateTime / TapDateTimeValue 处理
        if (value instanceof io.tapdata.entity.schema.value.DateTime) {
            return formatTapDateTime(value);
        }
        
        if (value instanceof io.tapdata.entity.schema.value.TapDateTimeValue) {
            return formatTapDateTimeValue(value);
        }
        
        // byte[] - 转换为 DuckDB blob 字面量格式
        if (value instanceof byte[]) {
            return formatByteArray((byte[]) value);
        }
        
        // Collection / Map - 转换为 JSON 字符串
        if (value instanceof Collection || value instanceof Map) {
            return formatJson(value);
        }
        
        // 默认：转换为字符串并转义
        logger.warn("Unknown type for SQL formatting: {}, using toString()", 
                value.getClass().getName());
        return formatString(value.toString());
    }
    
    /**
     * 格式化字符串值
     * 
     * <p>DuckDB 使用单引号包裹字符串，单引号内部用两个单引号转义。
     * 注意：DuckDB 的 standard_conforming_strings 默认是开启的，
     * 因此反斜杠不需要转义。</p>
     * 
     * @param str 字符串值
     * @return 格式化后的 SQL 字符串字面量
     */
    private static String formatString(String str) {
        // 只转义单引号（DuckDB 不需要转义反斜杠）
        String escaped = str.replace("'", "''");
        return "'" + escaped + "'";
    }
    
    /**
     * 格式化数值
     * 
     * @param number 数值
     * @return 格式化后的数值字符串
     */
    private static String formatNumber(Number number) {
        if (number instanceof BigDecimal) {
            // BigDecimal 需要去掉末尾多余的零
            return ((BigDecimal) number).stripTrailingZeros().toString();
        }
        if (number instanceof BigInteger) {
            return number.toString();
        }
        // Integer, Long, Double, Float 等直接输出
        return number.toString();
    }
    
    /**
     * 格式化 Timestamp
     * 
     * <p>DuckDB 支持多种时间戳格式，这里使用 'YYYY-MM-DD HH:MM:SS.SSS' 格式。</p>
     * 
     * @param timestamp 时间戳
     * @return 格式化后的 SQL 时间戳字面量
     */
    private static String formatTimestamp(Timestamp timestamp) {
        return "'" + TIMESTAMP_FORMATTER.format(timestamp.toLocalDateTime()) + "'";
    }
    
    /**
     * 格式化 Date
     * 
     * @param date 日期
     * @return 格式化后的 SQL 时间戳字面量
     */
    private static String formatDate(Date date) {
        return "'" + TIMESTAMP_FORMATTER.format(
                new Timestamp(date.getTime()).toLocalDateTime()) + "'";
    }
    
    /**
     * 格式化 LocalDateTime
     * 
     * @param dateTime 本地日期时间
     * @return 格式化后的 SQL 时间戳字面量
     */
    private static String formatLocalDateTime(LocalDateTime dateTime) {
        return "'" + TIMESTAMP_FORMATTER.format(dateTime) + "'";
    }
    
    /**
     * 格式化 LocalDate
     * 
     * <p>DuckDB 的 DATE 类型格式为 'YYYY-MM-DD'。</p>
     * 
     * @param date 本地日期
     * @return 格式化后的 SQL 日期字面量
     */
    private static String formatLocalDate(LocalDate date) {
        return "'" + DATE_FORMATTER.format(date) + "'";
    }
    
    /**
     * 格式化字节数组
     * 
     * <p>DuckDB 支持在字符串中使用 \x 转义序列表示 BLOB 数据。</p>
     * <p>例如：'\x48\x65\x6C\x6C\x6F' 表示 "Hello"</p>
     * 
     * @param bytes 字节数组
     * @return DuckDB blob 字面量（使用转义序列格式）
     */
    private static String formatByteArray(byte[] bytes) {
        StringBuilder blobLiteral = new StringBuilder();
        blobLiteral.append("'");
        for (byte b : bytes) {
            blobLiteral.append(String.format("\\x%02x", b));
        }
        blobLiteral.append("'");
        return blobLiteral.toString();
    }
    
    /**
     * 格式化 JSON（Collection 或 Map）
     * 
     * <p>将集合或映射转换为 JSON 字符串，并转义特殊字符。</p>
     * 
     * @param jsonObj 集合或映射对象
     * @return 格式化后的 SQL 字符串字面量
     */
    private static String formatJson(Object jsonObj) {
        // 注意：这里使用简单的 toString() 实现
        // 生产环境建议使用 Jackson 等 JSON 库
        String json = jsonObj.toString();
        return formatString(json);
    }
    
    /**
     * 格式化值为 CSV 格式（用于 COPY 语句）
     * 
     * @param value 字段值
     * @return CSV 格式字符串
     */
    public static String formatForCsv(Object value) {
        if (value == null) {
            return "";
        }
        
        String str = value.toString();
        
        // CSV 转义：双引号包裹，内部双引号转义为两个双引号
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        
        return str;
    }
    
    /**
     * 处理 Tapdata DateTime 类型
     */
    private static String formatTapDateTime(Object dateTime) {
        try {
            // 使用反射避免编译时依赖
            Object instant = dateTime.getClass().getMethod("toInstant").invoke(dateTime);
            long epochMilli = (Long) instant.getClass().getMethod("toEpochMilli").invoke(instant);
            return "'" + TIMESTAMP_FORMATTER.format(
                    new Timestamp(epochMilli).toLocalDateTime()) + "'";
        } catch (Exception e) {
            logger.warn("Failed to format TapDateTime: {}", e.getMessage());
            return formatString(dateTime.toString());
        }
    }
    
    /**
     * 处理 Tapdata TapDateTimeValue 类型
     */
    private static String formatTapDateTimeValue(Object tapDateTimeValue) {
        try {
            // 使用反射避免编译时依赖
            Object dateTime = tapDateTimeValue.getClass().getMethod("getValue").invoke(tapDateTimeValue);
            if (dateTime != null) {
                return formatTapDateTime(dateTime);
            }
            return "NULL";
        } catch (Exception e) {
            logger.warn("Failed to format TapDateTimeValue: {}", e.getMessage());
            return "NULL";
        }
    }
    
    /**
     * 字节数组转十六进制
     * 
     * @param bytes 字节数组
     * @return 十六进制字符串（小写）
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}

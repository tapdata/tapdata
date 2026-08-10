package io.tapdata.flow.engine.V2.node.duckdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 统一的类型转换器
 * 
 * <p>处理从数据类型到 Arrow Type 到 DuckDB Type 的所有转换</p>
 * 
 * <h3>转换链路：</h3>
 * <ul>
 *   <li>DataType String → ArrowType</li>
 *   <li>TapType → ArrowType</li>
 *   <li>TapField → ArrowType</li>
 *   <li>ArrowType → DuckDB Type String</li>
 * </ul>
 */
public class TypeConverter {

    private static final Logger logger = LoggerFactory.getLogger(TypeConverter.class);

    /** 数据类型到 ArrowType 的映射表 */
    private static final Map<String, ArrowType> DATA_TYPE_TO_ARROW = new HashMap<>();
    
    /** TapType 类名到 ArrowType 的映射表 */
    private static final Map<String, ArrowType> TAP_TYPE_TO_ARROW = new HashMap<>();

    static {
        // 初始化数据类型映射
        initDataTypeMappings();
        // 初始化 TapType 映射
        initTapTypeMappings();
    }

    /**
     * 初始化数据类型映射
     */
    private static void initDataTypeMappings() {
        // 整数类型
        DATA_TYPE_TO_ARROW.put("TINYINT", new ArrowType.Int(8, true));
        DATA_TYPE_TO_ARROW.put("SMALLINT", new ArrowType.Int(16, true));
        DATA_TYPE_TO_ARROW.put("INTEGER", new ArrowType.Int(32, true));
        DATA_TYPE_TO_ARROW.put("INT", new ArrowType.Int(32, true));
        DATA_TYPE_TO_ARROW.put("BIGINT", new ArrowType.Int(64, true));

        // 浮点数类型
        DATA_TYPE_TO_ARROW.put("FLOAT", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        DATA_TYPE_TO_ARROW.put("DOUBLE", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        DATA_TYPE_TO_ARROW.put("DECIMAL", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

        // 字符串类型
        DATA_TYPE_TO_ARROW.put("VARCHAR", new ArrowType.Utf8());
        DATA_TYPE_TO_ARROW.put("CHAR", new ArrowType.Utf8());
        DATA_TYPE_TO_ARROW.put("TEXT", new ArrowType.Utf8());
        DATA_TYPE_TO_ARROW.put("STRING", new ArrowType.Utf8());

        // 布尔类型
        DATA_TYPE_TO_ARROW.put("BOOLEAN", new ArrowType.Bool());
        DATA_TYPE_TO_ARROW.put("BOOL", new ArrowType.Bool());

        // 二进制类型
        DATA_TYPE_TO_ARROW.put("BLOB", new ArrowType.Binary());
        DATA_TYPE_TO_ARROW.put("BINARY", new ArrowType.Binary());
        DATA_TYPE_TO_ARROW.put("BYTEA", new ArrowType.Binary());

        // 日期时间类型
        DATA_TYPE_TO_ARROW.put("DATE", new ArrowType.Utf8());
        DATA_TYPE_TO_ARROW.put("TIME", new ArrowType.Utf8());
        DATA_TYPE_TO_ARROW.put("DATETIME", new ArrowType.Utf8());
        DATA_TYPE_TO_ARROW.put("TIMESTAMP", new ArrowType.Utf8());
    }

    /**
     * 初始化 TapType 映射
     */
    private static void initTapTypeMappings() {
        TAP_TYPE_TO_ARROW.put("TapNumber", new ArrowType.Int(64, true));
        TAP_TYPE_TO_ARROW.put("TapBoolean", new ArrowType.Bool());
        TAP_TYPE_TO_ARROW.put("TapString", new ArrowType.Utf8());
        TAP_TYPE_TO_ARROW.put("TapBinary", new ArrowType.Binary());
        TAP_TYPE_TO_ARROW.put("TapDate", new ArrowType.Utf8());
        TAP_TYPE_TO_ARROW.put("TapDateTime", new ArrowType.Utf8());
        TAP_TYPE_TO_ARROW.put("TapTime", new ArrowType.Utf8());
        TAP_TYPE_TO_ARROW.put("TapArray", new ArrowType.Utf8());
        TAP_TYPE_TO_ARROW.put("TapMap", new ArrowType.Utf8());
    }

    /**
     * 从 DataType 字符串转换到 ArrowType
     */
    public static ArrowType fromDataType(String dataType) {
        if (dataType == null || dataType.isBlank()) {
            return new ArrowType.Utf8();
        }
        
        String upperType = dataType.toUpperCase().trim();
        
        // 精确匹配
        ArrowType result = DATA_TYPE_TO_ARROW.get(upperType);
        if (result != null) {
            return result;
        }
        
        // 模糊匹配
        if (upperType.contains("INT")) {
            if (upperType.contains("BIG")) {
                return new ArrowType.Int(64, true);
            } else if (upperType.contains("SMALL")) {
                return new ArrowType.Int(16, true);
            } else if (upperType.contains("TINY")) {
                return new ArrowType.Int(8, true);
            }
            return new ArrowType.Int(32, true);
        }
        
        if (upperType.contains("FLOAT") || upperType.contains("DOUBLE") || upperType.contains("DECIMAL")) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        
        if (upperType.contains("BOOL")) {
            return new ArrowType.Bool();
        }
        
        if (upperType.contains("BLOB") || upperType.contains("BINARY")) {
            return new ArrowType.Binary();
        }
        
        if (upperType.contains("DATE") || upperType.contains("TIME")) {
            return new ArrowType.Utf8();
        }
        
        // 默认返回字符串类型
        return new ArrowType.Utf8();
    }

    /**
     * 从 TapType 转换到 ArrowType
     */
    public static ArrowType fromTapType(TapType tapType) {
        if (tapType == null) {
            return new ArrowType.Utf8();
        }
        
        String typeName = tapType.getClass().getSimpleName();
        ArrowType result = TAP_TYPE_TO_ARROW.get(typeName);
        
        if (result != null) {
            return result;
        }
        
        logger.debug("Unknown TapType: {}, defaulting to UTF8", typeName);
        return new ArrowType.Utf8();
    }

    /**
     * 从 TapType 类名转换到 ArrowType
     */
    public static ArrowType fromTapTypeName(String tapTypeName) {
        if (tapTypeName == null || tapTypeName.isBlank()) {
            return new ArrowType.Utf8();
        }
        
        ArrowType result = TAP_TYPE_TO_ARROW.get(tapTypeName);
        if (result != null) {
            return result;
        }
        
        return new ArrowType.Utf8();
    }

    /**
     * 从 TapField 转换到 ArrowType（优先使用 dataType）
     */
    public static ArrowType fromTapField(TapField tapField) {
        if (tapField == null) {
            return new ArrowType.Utf8();
        }
        
        // 优先使用 dataType
        String dataType = tapField.getDataType();
        if (dataType != null && !dataType.isBlank()) {
            return fromDataType(dataType);
        }
        
        // 其次使用 tapType
        TapType tapType = tapField.getTapType();
        if (tapType != null) {
            return fromTapType(tapType);
        }
        
        // 默认使用字符串类型
        return new ArrowType.Utf8();
    }

    /**
     * 从 dataType 转换到 DuckDB 类型字符串（用于 CREATE TABLE）
     */
    public static String toDuckDbType(String dataType) {
        if (dataType == null || dataType.isBlank()) {
            return "VARCHAR";
        }
        
        String upperType = dataType.toUpperCase().trim();
        
        // 整数类型
        if (upperType.contains("TINYINT")) {
            return "TINYINT";
        }
        if (upperType.contains("SMALLINT")) {
            return "SMALLINT";
        }
        if (upperType.contains("BIGINT")) {
            return "BIGINT";
        }
        if (upperType.contains("INT")) {
            return "INTEGER";
        }
        
        // 浮点数类型
        if (upperType.contains("FLOAT") || upperType.contains("DOUBLE") || upperType.contains("DECIMAL")) {
            return "DOUBLE";
        }
        
        // 布尔类型
        if (upperType.contains("BOOL")) {
            return "BOOLEAN";
        }
        
        // 二进制类型
        if (upperType.contains("BLOB") || upperType.contains("BINARY")) {
            return "BLOB";
        }
        
        // 日期时间类型
        if (upperType.contains("TIMESTAMP") || upperType.contains("DATETIME")) {
            return "TIMESTAMP";
        }
        if (upperType.contains("DATE")) {
            return "DATE";
        }
        if (upperType.contains("TIME")) {
            return "TIME";
        }
        
        // 默认字符串类型
        return "VARCHAR";
    }

    /**
     * 从 ArrowType 转换到 DuckDB 类型字符串
     */
    public static String toDuckDbType(ArrowType arrowType) {
        if (arrowType == null) {
            return "VARCHAR";
        }
        
        if (arrowType instanceof ArrowType.Int) {
            int bitWidth = ((ArrowType.Int) arrowType).getBitWidth();
            if (bitWidth <= 8) {
                return "TINYINT";
            } else if (bitWidth <= 16) {
                return "SMALLINT";
            } else if (bitWidth <= 32) {
                return "INTEGER";
            } else {
                return "BIGINT";
            }
        }
        
        if (arrowType instanceof ArrowType.FloatingPoint) {
            return "DOUBLE";
        }
        
        if (arrowType instanceof ArrowType.Bool) {
            return "BOOLEAN";
        }
        
        if (arrowType instanceof ArrowType.Binary) {
            return "BLOB";
        }
        
        // 默认返回字符串类型
        return "VARCHAR";
    }

    /**
     * 从 TapField 获取 DuckDB 类型字符串
     */
    public static String getDuckDbType(TapField tapField) {
        if (tapField == null) {
            return "VARCHAR";
        }
        
        // 优先使用 dataType
        String dataType = tapField.getDataType();
        if (dataType != null && !dataType.isBlank()) {
            return toDuckDbType(dataType);
        }
        
        // 其次使用 tapType
        ArrowType arrowType = fromTapField(tapField);
        return toDuckDbType(arrowType);
    }
    
    /**
     * 从 TapFieldDto 转换到 ArrowType（优先使用预计算类型）
     */
    public static ArrowType fromTapFieldDto(TapFieldDto dto) {
        if (dto == null) {
            return new ArrowType.Utf8();
        }
        
        // 优先使用预计算的 Arrow 类型
        if (dto.getArrowTypeName() != null) {
            return fromPrecomputedArrowType(dto);
        }
        
        // 其次使用 dataType
        if (dto.getDataType() != null && !dto.getDataType().isBlank()) {
            return fromDataType(dto.getDataType());
        }
        
        // 最后使用 tapTypeName
        if (dto.getTapTypeName() != null) {
            return fromTapTypeName(dto.getTapTypeName());
        }
        
        return new ArrowType.Utf8();
    }
    
    /**
     * 从预计算的类型信息构建 ArrowType
     */
    private static ArrowType fromPrecomputedArrowType(TapFieldDto dto) {
        String typeName = dto.getArrowTypeName();
        
        if ("Int".equals(typeName)) {
            int bitWidth = dto.getArrowBitWidth() != null ? dto.getArrowBitWidth() : 32;
            return new ArrowType.Int(bitWidth, true);
        }
        
        if ("FloatingPoint".equals(typeName)) {
            String precision = dto.getArrowPrecision();
            FloatingPointPrecision fpPrecision = FloatingPointPrecision.DOUBLE;
            if ("SINGLE".equals(precision)) {
                fpPrecision = FloatingPointPrecision.SINGLE;
            } else if ("HALF".equals(precision)) {
                fpPrecision = FloatingPointPrecision.HALF;
            }
            return new ArrowType.FloatingPoint(fpPrecision);
        }
        
        if ("Bool".equals(typeName)) {
            return new ArrowType.Bool();
        }
        
        if ("Binary".equals(typeName)) {
            return new ArrowType.Binary();
        }
        
        // 默认返回 Utf8
        return new ArrowType.Utf8();
    }
    
    /**
     * 从 TapFieldDto 获取 DuckDB 类型字符串（优先使用预计算类型）
     */
    public static String getDuckDbTypeFromDto(TapFieldDto dto) {
        if (dto == null) {
            return "VARCHAR";
        }
        
        // 优先使用预计算的 DuckDB 类型
        if (dto.getDuckDbTypeName() != null) {
            return dto.getDuckDbTypeName();
        }
        
        // 其次使用 dataType
        if (dto.getDataType() != null && !dto.getDataType().isBlank()) {
            return toDuckDbType(dto.getDataType());
        }
        
        // 最后使用 Arrow 类型
        ArrowType arrowType = fromTapFieldDto(dto);
        return toDuckDbType(arrowType);
    }
}

package com.tapdata.tm.commons.dag.process.dto;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TapField 的序列化友好 DTO
 * 
 * <p>用于替代 TapField，解决 TapField 中包含 ReadWriteLock 等不可序列化字段的问题。
 * 只包含初始化 NodeSchemaInfo 所需的字段。</p>
 */
public class TapFieldDto implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 字段名 */
    private String name;

    /** 原始字段名 */
    private String originalFieldName;

    /** 数据类型字符串（如 "VARCHAR", "INT"） */
    private String dataType;

    /** 是否主键 */
    private Boolean isPrimaryKey = false;

    /** 主键位置，从 1 开始 */
    private Integer primaryKeyPos;

    /** 是否可空 */
    private Boolean nullable = true;

    /** 字段位置，从 1 开始 */
    private Integer pos;

    /** TapType 类型名（如 "TapString", "TapNumber", "TapDateTime"） */
    private String tapTypeName;

    /** TapType 参数（如 length, precision, scale 等） */
    private Map<String, Object> tapTypeParams;
    
    /** Arrow 类型名称（预计算，如 "Int", "Utf8", "FloatingPoint"） */
    private String arrowTypeName;
    
    /** Arrow 类型位宽（对于 Int 类型） */
    private Integer arrowBitWidth;
    
    /** Arrow 类型精度（对于 FloatingPoint 类型，如 "DOUBLE", "SINGLE", "HALF"） */
    private String arrowPrecision;
    
    /** DuckDB 类型名称（预计算） */
    private String duckDbTypeName;

    public TapFieldDto() {
    }

    // ========== Getter & Setter ==========

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOriginalFieldName() {
        return originalFieldName;
    }

    public void setOriginalFieldName(String originalFieldName) {
        this.originalFieldName = originalFieldName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Boolean getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public Integer getPrimaryKeyPos() {
        return primaryKeyPos;
    }

    public void setPrimaryKeyPos(Integer primaryKeyPos) {
        this.primaryKeyPos = primaryKeyPos;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public Integer getPos() {
        return pos;
    }

    public void setPos(Integer pos) {
        this.pos = pos;
    }

    public String getTapTypeName() {
        return tapTypeName;
    }

    public void setTapTypeName(String tapTypeName) {
        this.tapTypeName = tapTypeName;
    }

    public Map<String, Object> getTapTypeParams() {
        return tapTypeParams;
    }

    public void setTapTypeParams(Map<String, Object> tapTypeParams) {
        this.tapTypeParams = tapTypeParams;
    }

    public String getArrowTypeName() {
        return arrowTypeName;
    }

    public void setArrowTypeName(String arrowTypeName) {
        this.arrowTypeName = arrowTypeName;
    }

    public Integer getArrowBitWidth() {
        return arrowBitWidth;
    }

    public void setArrowBitWidth(Integer arrowBitWidth) {
        this.arrowBitWidth = arrowBitWidth;
    }

    public String getArrowPrecision() {
        return arrowPrecision;
    }

    public void setArrowPrecision(String arrowPrecision) {
        this.arrowPrecision = arrowPrecision;
    }

    public String getDuckDbTypeName() {
        return duckDbTypeName;
    }

    public void setDuckDbTypeName(String duckDbTypeName) {
        this.duckDbTypeName = duckDbTypeName;
    }

    // ========== 构建辅助方法 ==========

    public TapFieldDto name(String name) {
        this.name = name;
        return this;
    }

    public TapFieldDto originalFieldName(String originalFieldName) {
        this.originalFieldName = originalFieldName;
        return this;
    }

    public TapFieldDto dataType(String dataType) {
        this.dataType = dataType;
        return this;
    }

    public TapFieldDto isPrimaryKey(Boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
        return this;
    }

    public TapFieldDto primaryKeyPos(Integer primaryKeyPos) {
        if (primaryKeyPos != null && primaryKeyPos > 0) {
            this.primaryKeyPos = primaryKeyPos;
            this.isPrimaryKey = true;
        }
        return this;
    }

    public TapFieldDto nullable(Boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public TapFieldDto pos(Integer pos) {
        this.pos = pos;
        return this;
    }

    public TapFieldDto tapTypeName(String tapTypeName) {
        this.tapTypeName = tapTypeName;
        return this;
    }

    public TapFieldDto tapTypeParams(Map<String, Object> tapTypeParams) {
        this.tapTypeParams = tapTypeParams;
        return this;
    }

    /**
     * 添加单个 TapType 参数
     */
    public TapFieldDto addTapTypeParam(String key, Object value) {
        if (this.tapTypeParams == null) {
            this.tapTypeParams = new LinkedHashMap<>();
        }
        this.tapTypeParams.put(key, value);
        return this;
    }

    public TapFieldDto arrowTypeName(String arrowTypeName) {
        this.arrowTypeName = arrowTypeName;
        return this;
    }

    public TapFieldDto arrowBitWidth(Integer arrowBitWidth) {
        this.arrowBitWidth = arrowBitWidth;
        return this;
    }

    public TapFieldDto arrowPrecision(String arrowPrecision) {
        this.arrowPrecision = arrowPrecision;
        return this;
    }

    public TapFieldDto duckDbTypeName(String duckDbTypeName) {
        this.duckDbTypeName = duckDbTypeName;
        return this;
    }

    /**
     * 预计算类型信息
     */
    public TapFieldDto precomputeTypes(String dataType) {
        if (dataType == null || dataType.isEmpty()) {
            return this;
        }
        
        String upperType = dataType.toUpperCase();
        
        // 预计算 Arrow 类型
        if (upperType.contains("TINYINT")) {
            this.arrowTypeName = "Int";
            this.arrowBitWidth = 8;
            this.duckDbTypeName = "TINYINT";
        } else if (upperType.contains("SMALLINT")) {
            this.arrowTypeName = "Int";
            this.arrowBitWidth = 16;
            this.duckDbTypeName = "SMALLINT";
        } else if (upperType.contains("BIGINT")) {
            this.arrowTypeName = "Int";
            this.arrowBitWidth = 64;
            this.duckDbTypeName = "BIGINT";
        } else if (upperType.contains("INT")) {
            this.arrowTypeName = "Int";
            this.arrowBitWidth = 32;
            this.duckDbTypeName = "INTEGER";
        } else if (upperType.contains("FLOAT") || upperType.contains("DOUBLE") || upperType.contains("DECIMAL")) {
            this.arrowTypeName = "FloatingPoint";
            this.arrowPrecision = "DOUBLE";
            this.duckDbTypeName = "DOUBLE";
        } else if (upperType.contains("BOOL")) {
            this.arrowTypeName = "Bool";
            this.duckDbTypeName = "BOOLEAN";
        } else if (upperType.contains("BLOB") || upperType.contains("BINARY") || upperType.contains("BYTEA")) {
            this.arrowTypeName = "Binary";
            this.duckDbTypeName = "BLOB";
        } else if (upperType.contains("DATE") || upperType.contains("TIME") || upperType.contains("TIMESTAMP")) {
            this.arrowTypeName = "Utf8";
            this.duckDbTypeName = "TIMESTAMP";
        } else {
            this.arrowTypeName = "Utf8";
            this.duckDbTypeName = "VARCHAR";
        }
        
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TapFieldDto that = (TapFieldDto) o;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "TapFieldDto{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", isPrimaryKey=" + isPrimaryKey +
                ", tapTypeName='" + tapTypeName + '\'' +
                '}';
    }
}

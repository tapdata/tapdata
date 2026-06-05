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

package com.tapdata.tm.commons.dag.process.dto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TapTable 的序列化友好 DTO
 * 
 * <p>用于替代 TapTable，解决 TapTable 中包含 ReadWriteLock 等不可序列化字段的问题。
 * 只包含初始化 NodeSchemaInfo 所需的字段。</p>
 */
public class TapTableDto implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 节点 ID（对应 TapTable.getId()） */
    private String id;

    /** 表名（对应 TapTable.getName()） */
    private String name;

    /** 主键列表 */
    private List<String> primaryKeys;

    /** 字段列表 */
    private List<TapFieldDto> fields;

    public TapTableDto() {
    }

    public TapTableDto(String id, String name) {
        this.id = id;
        this.name = name;
    }

    // ========== Getter & Setter ==========

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<TapFieldDto> getFields() {
        return fields;
    }

    public void setFields(List<TapFieldDto> fields) {
        this.fields = fields;
    }

    // ========== 构建辅助方法 ==========

    public TapTableDto id(String id) {
        this.id = id;
        return this;
    }

    public TapTableDto name(String name) {
        this.name = name;
        return this;
    }

    public TapTableDto primaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public TapTableDto addPrimaryKey(String primaryKey) {
        if (this.primaryKeys == null) {
            this.primaryKeys = new ArrayList<>();
        }
        this.primaryKeys.add(primaryKey);
        return this;
    }

    public TapTableDto fields(List<TapFieldDto> fields) {
        this.fields = fields;
        return this;
    }

    public TapTableDto addField(TapFieldDto field) {
        if (this.fields == null) {
            this.fields = new ArrayList<>();
        }
        this.fields.add(field);
        return this;
    }

    /**
     * 获取字段名 → TapFieldDto 的映射（方便查询）
     */
    public java.util.Map<String, TapFieldDto> getNameFieldMap() {
        if (fields == null) {
            return Collections.emptyMap();
        }
        java.util.Map<String, TapFieldDto> map = new java.util.LinkedHashMap<>();
        for (TapFieldDto field : fields) {
            if (field.getName() != null) {
                map.put(field.getName(), field);
            }
        }
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TapTableDto that = (TapTableDto) o;
        // 只比较 id 和 name（用于 @EqField 对比）
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TapTableDto{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", primaryKeys=" + primaryKeys +
                ", fieldCount=" + (fields != null ? fields.size() : 0) +
                '}';
    }
}

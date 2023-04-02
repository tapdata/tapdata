package com.tapdata.tm.commons.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.schema.bean.Relation;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.result.ResultItem;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/9 下午4:11
 */
@Getter
@Setter
public class Schema implements Cloneable, Serializable {

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId id;


    private String taskId;
    private String nodeId;
    @JsonProperty("qualified_name")
    private String qualifiedName;
    @JsonProperty("meta_type")
    private String metaType;
    @JsonProperty("is_deleted")
    private boolean isDeleted = false;
    @JsonProperty("original_name")
    private String originalName;
    private String ancestorsName;
    @JsonProperty("dev_version")
    private Integer devVersion;
    private String databaseId;
    private String schemaVersion;
    private Integer version;
    private String comment;
    private String name;
    private String lienage;
    private List<Relation> relation;
    @JsonProperty("fields_lienage")
    private String fieldsLienage;
    private List<Field> fields;
    private List<Map<String, Object>> indexes;
    private String createSource;
    private Boolean virtual;
    private List<Tag> classifications;

    private List<TableIndex> indices;
    @JsonProperty("alias_name")
    private String aliasName;
    @JsonProperty("custom_properties")
    private Map<String, Object> customProperties;

    private Set<Integer> partitionSet;

    //下面几个只有历史版本中存在的字段
    @JsonProperty("version_user_id")
    private String versionUserId;
    @JsonProperty("version_user_name")
    private String versionUserName;
    @JsonProperty("version_description")
    private String versionDescription;

    @JsonProperty("version_time")
    private Date versionTime;
    private Object pipline;
    private com.tapdata.tm.commons.schema.bean.Schema schema;

    //查询'collection', 'table', 'view', 'mongo_view' 等metatype的时候需要设置的属性
    private String database;
    private String username;
    private String charset;

    private List<String> invalidFields; // 无效的字段名称列表，将有问题的字段名称统一记录下来

    private ObjectId oldId;

    public Schema copy() throws CloneNotSupportedException {
        return (Schema)clone();
    }

    private String sourceNodeDatabaseType;
    //pdk的模型加载的版本控制删除的唯一时间标记
    private Long lastUpdate;
    private String storageEngine;
    protected String pdkId;
    protected String pdkGroup;
    protected String pdkVersion;

    //逻辑表物理表分离所添加的相关属性
    private String linkTaskId;

    private boolean hasPrimaryKey;
    private boolean hasUnionIndex;
    private boolean hasTransformEx;
    private Map<String, PossibleDataTypes> findPossibleDataTypes;
    private boolean hasUpdateField;

    /**
     * 是否是虚拟表 'virtual' 'source'
     */
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String sourceType;

    public void setSourceNodeDatabaseType(String sourceNodeDatabaseType) {
        this.sourceNodeDatabaseType = sourceNodeDatabaseType;
    }

    public String getSourceNodeDatabaseType() {
        return sourceNodeDatabaseType;
    }

}

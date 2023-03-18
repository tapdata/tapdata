package com.tapdata.tm.commons.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.bean.DataRules;
import com.tapdata.tm.commons.schema.bean.Relation;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.SourceTypeEnum;
import com.tapdata.tm.commons.schema.bean.*;
import io.tapdata.entity.conversion.PossibleDataTypes;
import io.tapdata.entity.result.ResultItem;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.stream.Collectors;


/**
 * 数据源模型
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataInstancesDto extends BaseDto {
    @JsonProperty("qualified_name")
    private String qualifiedName;
    @JsonProperty("meta_type")
    private String metaType;
    @JsonProperty("is_deleted")
    private boolean isDeleted = false;
    @JsonProperty("original_name")
    private String originalName;
    // 原始表名
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
    private boolean hasPrimaryKey;
    private Object indexes;
    private SourceDto source;
    private String createSource;
    private Boolean virtual;
    private List<Tag> listtags;
    @JsonProperty("last_user_name")
    private String lastUserName;
    private List<TableIndex> indices;
    private boolean hasUnionIndex;
    private Set<Integer> partitionSet;
    private FileProperty fileProperty;
    private List<String> fromFile;
    @JsonProperty("alias_name")
    private String aliasName;
    @JsonProperty("custom_properties")
    private Map<String, Object> customProperties;

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
    //添加修改的时候，可能会作为数据源的id传参，不会入库
    private String connectionId;

    //查询'database', 'directory', 'ftp' 等metatype的时候需要设置的属性
    private List<MetadataInstancesDto> collections;

    //查询'collection', 'table', 'view', 'mongo_view' 等metatype的时候需要设置的属性
    private String database;
    private String username;
    private String transformUuid;


    private List<MetadataInstancesDto> histories;
    @JsonProperty("data_rules")
    private DataRules dataRules;
    private Long lastUpdate;
    private String storageEngine;
    private String charset;
    protected String pdkId;
    protected String pdkGroup;
    protected String pdkVersion;

    //auto ddl need this param
    private Long tmCurrentTime;
    //auto ddl need this param
    private String taskId;

    private String nodeId;

     private String description;


    /**
     * 是否是虚拟表 'virtual' 'source'
     */
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private String sourceType= SourceTypeEnum.SOURCE.name();

    private ObjectId oldId;
    private List<ResultItem> resultItems;
    private Map<String, PossibleDataTypes> findPossibleDataTypes;
    private boolean hasUpdateField;

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    public String getAncestorsName() {
        return Objects.isNull(ancestorsName) ? originalName : ancestorsName;
    }

    public static void sortField(List<Field> fields) {
        if (CollectionUtils.isNotEmpty(fields)) {
            List<Field> noPrimarys = fields.stream().filter(f -> {
                Boolean primaryKey = f.getPrimaryKey();
                return primaryKey == null || !primaryKey;
            }).collect(Collectors.toList());
            fields.removeAll(noPrimarys);
            fields.sort(Comparator.comparing(Field::getFieldName));
            if (CollectionUtils.isNotEmpty(noPrimarys)) {
                noPrimarys.sort(Comparator.comparing(Field::getFieldName));
                fields.addAll(noPrimarys);
            }
        }
    }
}


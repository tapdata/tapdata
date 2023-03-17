package com.tapdata.tm.metadatainstance.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.schema.FileProperty;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.schema.bean.DataRules;
import com.tapdata.tm.commons.schema.bean.Relation;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.metadatainstance.vo.SourceTypeEnum;
import io.tapdata.entity.result.ResultItem;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 数据源模型
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("MetadataInstances")
public class MetadataInstancesEntity extends BaseEntity {
    @Field("qualified_name")
    private String qualifiedName;
    private String ancestorsName;
    @Field("meta_type")
    private String metaType;
    @Field("is_deleted")
    private boolean isDeleted = false;
    @Field("original_name")
    private String originalName;
    @Field("dev_version")
    private Integer devVersion;
    private String databaseId;
    private String schemaVersion;
    private Integer version;
    private String comment;
    private String name;
    private String lienage;
    private List<Relation> relation;
    private List<Tag> listtags;
    @Field("fields_lienage")
    private String fieldsLienage;
    private List<com.tapdata.tm.commons.schema.Field> fields;
    private boolean hasPrimaryKey;
    private Object indexes;
    @Field("source")
    private SourceDto source;
    private String createSource;
    private Boolean virtual;
    private List<Tag> classifications;
    @Field("last_user_name")
    private String lastUserName;
    private List<TableIndex> indices;
    private boolean hasUnionIndex;
    private Set<Integer> partitionSet;
    private FileProperty fileProperty;
    private List<String> fromFile;
    @Field("alias_name")
    private String aliasName;
    @Field("custom_properties")
    private Map<String, Object> customProperties;

    //下面几个只有历史版本中存在的字段
    @Field("version_user_id")
    private String versionUserId;
    @Field("version_user_name")
    private String versionUserName;
    @Field("version_description")
    private String versionDescription;

    @Field("version_time")
    private Date versionTime;
    private Object pipline;
    private Schema schema;
    //查询'database', 'directory', 'ftp' 等metatype的时候需要设置的属性
    private List<MetadataInstancesDto> collections;

    //查询'collection', 'table', 'view', 'mongo_view' 等metatype的时候需要设置的属性
    private String database;
    private String username;

    private List<MetadataInstancesDto> histories;
    @Field("data_rules")
    private DataRules dataRules;
    private Long lastUpdate;
    private String storageEngine;
    private String charset;
    protected String pdkId;
    protected String pdkGroup;
    protected String pdkVersion;

    private Long tmCurrentTime;
    private String transformUuid;

    /**
     * 是否是虚拟表 'virtual' 'source'
     */
    private String sourceType= SourceTypeEnum.SOURCE.name();

    //逻辑表物理表分离所添加的相关属性
    private String taskId;

    private String nodeId;
    private List<ResultItem> resultItems;
    private boolean hasUpdateField;
}

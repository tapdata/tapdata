package com.tapdata.tm.metadatainstance.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.schema.bean.DataRules;
import com.tapdata.tm.commons.schema.bean.Relation;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据目录详情 返回类
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class TableListVo extends BaseVo {

    @JsonProperty("qualified_name")
    private String qualifiedName;

    @JsonProperty("meta_type")
    private String metaType;

    @JsonProperty("is_deleted")
    private boolean isDeleted = false;

    @JsonProperty("original_name")
    private String originalName;
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
    private List<String> indexes;
    private SourceDto source;
    private String createSource;
    private Boolean virtual;
    private List<Tag> listtags;
    @JsonProperty("last_user_name")
    private String lastUserName;
    private List<TableIndex> indices;
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
    private Schema schema;
    //添加修改的时候，可能会作为数据源的id传参，不会入库
    private String connectionId;

    //只要在前端选了数据库的时候，才会用这个字段返回所有的表，现在前端已经没有数据库这个选择了，所以该字段已废弃
    @Deprecated
    private List<Map<String,String>> collections;

    //查询'collection', 'table', 'view', 'mongo_view' 等metatype的时候需要设置的属性
    private String database;
    private String username;


    private List<MetadataInstancesDto> histories;
    @JsonProperty("data_rules")
    private DataRules dataRules;
}

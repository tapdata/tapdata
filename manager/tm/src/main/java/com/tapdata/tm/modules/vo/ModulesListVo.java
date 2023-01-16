package com.tapdata.tm.modules.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;


@Data
@EqualsAndHashCode(callSuper=false)
public class ModulesListVo extends BaseVo {
    private String name;

    @JsonProperty("datasource")
    private String dataSource;

    @JsonProperty("tableName")
    private String tableName;

    private String apiVersion;

    private String basePath;

    private String readPreference;
    private String readPreferenceTag;

    private String readConcern;


    private String description;

    private String describtion;

    private String prefix;

    private String path;
    private String apiType;

    private String status;
    private String createUser;
    private List<Path> paths;
    private List<Field> fields;

    @JsonProperty("listtags")
    private List listTags;

    private String project;
    private String createType;

    private String connectionId;

    private String connection;

    private Source source;

    /**
     * 创建者
     */
    private String user;

    @JsonProperty("last_updated")
    private Date lastUpdAt;

    private String operationType;

    private String connectionType;

    private String connectionName;
}

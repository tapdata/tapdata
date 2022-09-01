package com.tapdata.tm.modules.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.entity.Path;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;

import java.util.Date;
import java.util.List;


/**
 * Modules
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ModulesDto extends BaseDto {
    private String name;

    @JsonProperty("datasource")
    private String dataSource;

    @JsonProperty("tablename")
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

    private List<Path> paths;
    private List<Field> fields;

    @JsonProperty("listtags")
    private List listTags;

    private String project;
    private String createType;

    private String connectionId;

    private ObjectId connection;
    private String access_token;
    private String user;

    @Deprecated
    private ObjectId user_id;

    private Date last_updated;
    private String Email;

    //请求失败率
    private Number failRate;
    private Long resRows;

    private Long visitCount;
    private Long latency;
    private Long responseTime;

    private Long reqBytes;
    private Boolean isDeleted;
}

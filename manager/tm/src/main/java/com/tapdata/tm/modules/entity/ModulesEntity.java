package com.tapdata.tm.modules.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import javax.validation.constraints.NotBlank;
import java.util.List;


/**
 * Modules
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Modules")
public class ModulesEntity extends BaseEntity {
    private String name;

    @Field("datasource")
    @JsonProperty("datasource")
    private String dataSource;

//    @Field("tablename")
    @JsonProperty("tableName")
    private String tableName;

    private String apiVersion;

    private String basePath;

    private String readPreference;

    private String readConcern;


    private String describtion;

    private String prefix;
    private String project;

//    private String path;
    private String apiType;

    private String status;

    private List<Path> paths;

    private List<com.tapdata.tm.commons.schema.Field> fields;

    private List<Tag> listtags;

    private String createType;

    @Field("is_deleted")
    private Boolean isDeleted;

    // 总的访问行数
    @Field("res_rows")
    private Long resRows;

    //总的访问次数
    private Long visitCount;

    private ObjectId connection;

    //请求失败率
    private Number failRate;

    private Long responseTime;

    private Long latency;

    @Field("req_bytes")
    private Long reqBytes;

    private String connectionId;

    private String operationType;

    private String connectionType;

    private String connectionName;

    private String description;
    /** 访问路径方式  默认值 default  自定义 customize*/
    private String pathAccessMethod;
}

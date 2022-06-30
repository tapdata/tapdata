package com.tapdata.tm.modules.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.vo.BaseVo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper=false)
public class ModulesDetailVo extends BaseVo {
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

    private String connection;
    private String access_token;
    private String user;
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

    private DataSourceConnectionDto source;

}

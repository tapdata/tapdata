package com.tapdata.tm.metadatainstance.vo;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.modules.entity.Path;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

@Data
public class ExportModulesVo {
    private String id;

    private String customId;

    private Date createAt;

    private Date lastUpdAt;

    private String lastUpdBy;
    private String createUser;
    private String name;

    private String dataSource;

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

    private List listTags;

    private String project;
    private String createType;

    private String connectionId;

    private String connection;
    private String access_token;
    private String user;
    private String user_id;

    private Date last_updated;

    //请求失败率
    private Number failRate;
    private Long resRows;

    private Long visitCount;
    private Long latency;
    private Long responseTime;

    private Long reqBytes;
}

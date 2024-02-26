package com.tapdata.tm.Settings.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Data
@Document("Settings") // 对应数据库表
public class Settings {
    @Id
    @Field("_id")
    private String id;

    private String category;

    private Integer category_sort;
    private Object default_value;
    private String documentation;
    private Boolean hot_reloading;

    private String key;

    private String key_label;
    private Integer last_update;
    private String last_update_by;
    private String scope;
    private Integer sort;
    private Boolean user_visible;
    private Object value;
    private String mask;
    private List<String> values;
    private Boolean isArray;
    private List<String> enums;

}

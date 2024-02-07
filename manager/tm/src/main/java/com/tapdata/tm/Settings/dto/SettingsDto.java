package com.tapdata.tm.Settings.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class SettingsDto{
    @JsonProperty("id")
    private String id;

    private String category;

    private String key;

    private Object value;

    private Object default_value;

    private String documentation;

    private long last_update;

    private String last_update_by;

    private String scope;

    private String category_sort;

    private String sort;

    private String key_label;

    private boolean user_visible;

    private boolean hot_reloading;
    private List<String> enums;

    private Integer min;

}

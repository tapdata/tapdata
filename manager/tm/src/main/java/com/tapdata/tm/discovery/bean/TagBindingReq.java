package com.tapdata.tm.discovery.bean;

import lombok.Data;

import java.util.List;

@Data
public class TagBindingReq {
    private List<TagBindingParam> tagBindingParams;
    private List<String> tagIds;

    private List<String> oldTagIds;
}

package com.tapdata.tm.task.service.batchin.dto.project.content;

import lombok.Data;

import java.util.Map;

@Data
public class Content {
    protected Object settings;
    protected Map<String, Collection> collections;
    protected Map<String, ContentMapping> mappings;
    protected Map<String, Object> relationships;
    protected Map<String, Object> diagrams;
}

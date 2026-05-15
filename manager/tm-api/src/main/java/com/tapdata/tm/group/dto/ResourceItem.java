package com.tapdata.tm.group.dto;

import lombok.Data;
import org.springframework.data.annotation.Transient;


@Data
public class ResourceItem {
    private String id;
    private ResourceType type;
    @Transient
    private String name;
}

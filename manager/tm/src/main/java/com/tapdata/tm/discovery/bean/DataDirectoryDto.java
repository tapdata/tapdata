package com.tapdata.tm.discovery.bean;

import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;

import java.util.List;

@Data
public class DataDirectoryDto {
    private String id;
    private String name;
    private String type;
    private String desc;
    private List<Tag> listtags;
    private List<Tag> allTags;
}

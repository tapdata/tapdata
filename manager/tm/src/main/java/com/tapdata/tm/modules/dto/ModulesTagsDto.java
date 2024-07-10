package com.tapdata.tm.modules.dto;

import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;

import java.util.List;

@Data
public class ModulesTagsDto {
    private String moduleId;
    private List<Tag> listtags;
}

package com.tapdata.tm.metadatainstance.param;

import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;

import java.util.List;

@Data
public class ClassificationParam {
    private String id;
    List<Tag> classifications;
}

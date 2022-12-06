package com.tapdata.tm.metadatadefinition.param;

import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class BatchUpdateParam {
    List<String> id;
    private List<Tag> listtags;
}

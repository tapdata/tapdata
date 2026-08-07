package com.tapdata.tm.task.param;

import com.tapdata.tm.commons.schema.Tag;
import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
public class BatchApplyListTagsParam {
    @JsonAlias("taskIds")
    private List<String> ids;
    private List<TagState> tags;

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class TagState extends Tag {
        private String desired;
    }
}

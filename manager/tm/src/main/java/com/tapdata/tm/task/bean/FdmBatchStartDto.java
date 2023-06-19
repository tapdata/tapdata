package com.tapdata.tm.task.bean;

import lombok.Data;

import java.util.List;

@Data
public class FdmBatchStartDto {
    private String tagId;
    private List<String> taskIds;
}

package com.tapdata.tm.group.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class GroupInfoRecordDto extends BaseDto {
    public static final String TYPE_EXPORT = "export";
    public static final String TYPE_IMPORT = "import";
    public static final String STATUS_EXPORTING = "exporting";
    public static final String STATUS_IMPORTING = "importing";
    public static final String STATUS_COMPLETED = "completed";
    public static final String STATUS_FAILED = "failed";

    private String type;
    private Date operationTime;
    private String operator;
    private String status;
    private String message;
    private String fileName;
    private List<GroupInfoRecordDetail> details;
}

package com.tapdata.tm.task.vo;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper=false)
public class SubTaskDetailVo extends SubTaskDto {
    private Date startTime;
    private String creator;
}

package com.tapdata.tm.commons.task.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.DagDeserialize;
import com.tapdata.tm.commons.base.convert.DagSerialize;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.dag.DAG;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * Task
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FlowTaskDto extends ParentTaskDto {

    private Dag dag;
    private Dag temp;

}

package com.tapdata.tm.task.param;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.DagDeserialize;
import com.tapdata.tm.commons.base.convert.DagSerialize;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Date;


/**
 * Task
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskParam extends ParentTaskDto {

    /** 任务图*/
    @JsonSerialize( using = DagSerialize.class)
    @JsonDeserialize( using = DagDeserialize.class)
    private DAG dag;


    /**
     * cacheName 缓存名称
     * cacheKeys  字符串 ，用逗号隔开
     * maxRows 最大记录数
     * ttl 过期时间 秒为单位
     */
    private Boolean shareCache=true;
}

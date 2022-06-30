package com.tapdata.tm.inspect.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.Map;


/**
 * 数据校验详情
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("InspectDetails")
public class InspectDetailsEntity extends BaseEntity {
    private String inspect_id;
    private String taskId;
    private String type;
    private Map<String, Object> source;
    private Map<String, Object> target;
    private String inspectResultId;

    /**
     * 高级校验: 用户执行脚本后返回的错误信息
     */
    private String message;

    private Date ttlTime;

}

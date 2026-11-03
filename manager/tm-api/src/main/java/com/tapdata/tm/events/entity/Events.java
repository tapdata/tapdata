package com.tapdata.tm.events.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@EqualsAndHashCode(callSuper=false)
@Document("Events")
public class Events extends BaseEntity {
    private String groupId;

    private EventData event_data;

    private FailResult failed_result;

    /**
     * {@link #failed_result} 的别名访问器。
     *
     * <p>EventsDto 的字段名为 failResult，转换依赖 BeanUtils.copyProperties 的属性名匹配；
     * 提供同名 getter/setter 后，失败的投递信息才能带到重试链路。
     */
    public FailResult getFailResult() {
        return failed_result;
    }

    public void setFailResult(FailResult failResult) {
        this.failed_result = failResult;
    }

    private String event_status;
    private String job_id;
    private  String name;
    private String receivers;


    private String tag;
    private String type;
    private String sendGroupId;
    private  Long sendGroupTTL;
    private Boolean lock;



}

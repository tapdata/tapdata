package com.tapdata.tm.inspect.entity;

import com.tapdata.tm.base.entity.SchedulableEntity;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.inspect.bean.Limit;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.inspect.bean.Timing;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;


/**
 * 校验任务
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Inspect")
public class InspectEntity extends SchedulableEntity {
    private String name;        // 校验任务名称
    private String flowId;      // 任务ID
    private String mode;        // 运行方式: 手工执行, 定时调度执行, manual/cron
    private String inspectMethod;       // 校验方法，row_count: 行数校验；field：字段校验
    private String inspectDifferenceMode;// 差异结果模式：All(输出所有差异),OnSourceExists(只输出源存在的异常数据)
    private Timing timing;   // 定时调度表达式，mode = cron 时，需要配置
    private Limit limit;             //
    private List<Task> tasks;        // 校验任务明细

    private Boolean enabled=true;         // 启用, 禁用校验；禁用时，重复任务都不在执行

    private String byFirstCheckId; // 如果是差异校验，需要指定对应批次第一次校验结果编号

    private String status;          // "pause/scheduling/running/error/done",    // 任务状态
    private String errorMsg;        // 状态为 error 时有效

    private Long ping_time;
    /**  */
    private Long lastStartTime;
    private String result;
    private PlatformInfo platformInfo;
    private  Boolean is_deleted;
    private String taskId;
}

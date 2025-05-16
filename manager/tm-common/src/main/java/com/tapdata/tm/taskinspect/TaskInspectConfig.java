package com.tapdata.tm.taskinspect;


import com.tapdata.tm.taskinspect.config.Custom;
import com.tapdata.tm.taskinspect.config.IConfig;
import com.tapdata.tm.taskinspect.config.Intelligent;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/12 16:07 Create
 */
@Getter
@Setter
public class TaskInspectConfig implements IConfig<TaskInspectConfig> {
    private Boolean enable;            // 是否开启校验
    private TaskInspectMode mode;      // 校验模式
    private Intelligent intelligent;   // 智能校验配置
    private Custom custom;             // 自定义校验配置
    private Integer queueCapacity;     // 队列最大容量
    private Long cdcTimeout;           // 增量校验在队列中超时，不作延迟等待
    private Integer cdcMaxUpdateTimes; // 增量最大更新次数，超过时长进入高优校验队列

    @Override
    public TaskInspectConfig init(int depth) {
        setEnable(init(getEnable(), false));
        setMode(init(getMode(), TaskInspectMode.CUSTOM));
        setCustom(init(getCustom(), depth, Custom.class));
        setIntelligent(init(getIntelligent(), depth, Intelligent.class));
        setQueueCapacity(init(getQueueCapacity(), 1000));
        setCdcTimeout(init(getCdcTimeout(), 60 * 1000L));
        setCdcMaxUpdateTimes(init(getCdcMaxUpdateTimes(), 10));
        return this;
    }
}

package com.tapdata.tm.taskinspect.config;

import com.tapdata.tm.taskinspect.cons.CustomFullTriggerEnum;
import com.tapdata.tm.taskinspect.cons.CustomFullTypeEnum;
import lombok.Getter;
import lombok.Setter;

/**
 * 任务校验-全量校验配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 18:16 Create
 */
@Getter
@Setter
public class CustomFull implements IConfig<CustomFull> {
    private Boolean enable;
    private CustomFullTriggerEnum trigger; // 触发方式
    private CustomFullTypeEnum type; // 校验方式
    private String cron; // 调度表达式
    private Double samplePercent; // 抽样百分比
    private Long sampleLimit; // 抽样上限

    @Override
    public CustomFull init(int depth) {
        setEnable(init(getEnable(), false));
        setTrigger(init(getTrigger(), CustomFullTriggerEnum.AUTO));
        setType(init(getType(), CustomFullTypeEnum.HASH));
        setCron(init(getCron(), ""));
        setSamplePercent(init(getSamplePercent(), 0.1));
        setSampleLimit(init(getSampleLimit(), 100000000L));
        return this;
    }
}

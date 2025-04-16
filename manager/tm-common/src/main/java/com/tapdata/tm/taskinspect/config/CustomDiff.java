package com.tapdata.tm.taskinspect.config;

import lombok.Getter;
import lombok.Setter;

/**
 * 任务校验-差异校验校验配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 18:16 Create
 */
@Getter
@Setter
public class CustomDiff implements IConfig<CustomDiff> {
    private Boolean enable; // 是否开启差异校验
    private Long limit; // 差异存储上限
    private Integer tryTimes; // 差异校验次数

    @Override
    public CustomDiff init(int depth) {
        setEnable(init(getEnable(), false));
        setLimit(init(getLimit(), 1000L));
        setTryTimes(init(getTryTimes(), 2));
        return this;
    }

}

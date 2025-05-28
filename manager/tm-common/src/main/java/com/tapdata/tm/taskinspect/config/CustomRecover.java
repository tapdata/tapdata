package com.tapdata.tm.taskinspect.config;

import lombok.Getter;
import lombok.Setter;

/**
 * 任务校验-数据修复配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 18:16 Create
 */
@Getter
@Setter
public class CustomRecover implements IConfig<CustomRecover> {
    private Boolean enable;

    @Override
    public CustomRecover init(int depth) {
        setEnable(init(getEnable(), false));
        return this;
    }
}

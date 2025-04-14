package com.tapdata.tm.taskinspect;


import com.tapdata.tm.taskinspect.config.Custom;
import com.tapdata.tm.taskinspect.config.IConfig;
import com.tapdata.tm.taskinspect.config.Intelligent;
import lombok.Data;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/12 16:07 Create
 */
@Data
public class TaskInspectConfig implements IConfig<TaskInspectConfig> {
    private Boolean enable; // 是否开启校验
    private TaskInspectMode mode; // 校验模式
    private Intelligent intelligent; // 智能校验配置
    private Custom custom; // 自定义校验配置

    @Override
    public TaskInspectConfig init(int depth) {
        setEnable(init(getEnable(), false));
        setMode(init(getMode(), TaskInspectMode.CUSTOM));
        setCustom(init(getCustom(), depth, Custom.class));
        setIntelligent(init(getIntelligent(), depth, Intelligent.class));
        return this;
    }
}

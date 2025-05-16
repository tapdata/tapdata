package com.tapdata.tm.taskinspect.config;

import com.tapdata.tm.taskinspect.cons.CustomCdcTypeEnum;
import lombok.Getter;
import lombok.Setter;

/**
 * 任务校验 - 增量校验配置
 * 用于配置和管理自定义 CDC (变更数据捕获) 的相关参数
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 18:16 Create
 */
@Getter
@Setter
public class CustomCdc implements IConfig<CustomCdc> {
    /**
     * 是否启用自定义 CDC 校验
     */
    private Boolean enable;

    /**
     * 校验方式类型
     */
    private CustomCdcTypeEnum type;

    /**
     * 抽样方式配置对象
     */
    private CustomCdcSample sample;

    /**
     * 初始化配置
     * 设置默认值并初始化相关配置对象
     *
     * @param depth 初始化深度
     * @return 返回初始化后的当前对象
     */
    @Override
    public CustomCdc init(int depth) {
        setEnable(init(getEnable(), false)); // 默认禁用
        setType(init(getType(), CustomCdcTypeEnum.SAMPLE)); // 默认使用采样方式
        setSample(init(getSample(), depth, CustomCdcSample.class)); // 初始化采样配置
        return this;
    }
}

package com.tapdata.tm.taskinspect.config;

import lombok.Data;

/**
 * 校验配置-智能模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 17:02 Create
 */
@Data
public class Intelligent implements IConfig<Intelligent> {

    private Boolean enable; // 是否开启校验
    private Long fullDiffLimit; // 全量差异保存上限
    private Long fullSampleLimit; // 全量触发抽样阈值
    private Long cdcSampleLimit; // 增量抽样阈值
    private Integer cdcSampleInterval; // 增量抽样间隔(秒)
    private Boolean enableRecover; // 开启自动修复

    @Override
    public Intelligent init(int depth) {
        setEnable(init(getEnable(), false));
        setFullDiffLimit(init(getFullDiffLimit(), 10000L));
        setFullSampleLimit(init(getFullSampleLimit(), 100000000L));
        setCdcSampleLimit(init(getCdcSampleLimit(), 10L));
        setCdcSampleInterval(init(getCdcSampleInterval(), 1));
        setEnableRecover(init(getEnableRecover(), false));
        return this;
    }
}

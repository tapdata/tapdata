package com.tapdata.tm.taskinspect.config;

import lombok.Data;

/**
 * 校验配置-自定义模式
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 17:02 Create
 */
@Data
public class Custom implements IConfig<Custom> {

    private CustomFull full; // 全量校验配置
    private CustomCdc cdc; // 增量校验配置
    private CustomDiff diff; // 差异校验
    private CustomRecover recover; // 数据修复

    @Override
    public Custom init(int depth) {
        setFull(init(getFull(), depth, CustomFull.class));
        setCdc(init(getCdc(), depth, CustomCdc.class));
        setDiff(init(getDiff(), depth, CustomDiff.class));
        setRecover(init(getRecover(), depth, CustomRecover.class));
        return this;
    }
}

package com.tapdata.tm.taskinspect.config;

import lombok.Data;

/**
 * 自定义增量抽样-校验配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/17 18:16 Create
 */
@Data
public class CustomCdcSample implements IConfig<CustomCdcSample> {
    private Integer capacity; // 缓存上限
    private Integer limit; // 抽样上限
    private Integer interval; // 抽样间隔(秒)

    @Override
    public CustomCdcSample init(int depth) {
        setCapacity(init(getCapacity(), 100));
        setLimit(init(getLimit(), 10));
        setInterval(init(getInterval(), 1));
        return this;
    }

    public void fill(CustomCdcSample config) {
        setCapacity(config.getCapacity());
        setLimit(config.getLimit());
        setInterval(config.getInterval());
    }
}

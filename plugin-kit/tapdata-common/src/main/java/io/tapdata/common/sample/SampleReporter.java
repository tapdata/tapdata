package io.tapdata.common.sample;

import java.util.Map;

/**
 * 每种采集器， 每个周期执行的所有结果
 */
public interface SampleReporter {
    /**
     * 每个采样周期回调该接口
     * key是业务指定的id
     * value是每个采样周期的计算结构
     *
     * @param pointValues 会被PointCollector复用， 需要业务层同步处理
     * @param tags
     */
    void execute(Map<String, Number> pointValues, Map<String, String> tags);
}

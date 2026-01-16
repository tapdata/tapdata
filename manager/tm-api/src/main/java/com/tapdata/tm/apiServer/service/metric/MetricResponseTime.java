package com.tapdata.tm.apiServer.service.metric;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.utils.PercentileCalculator;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiServer.vo.metric.MetricDataBase;
import com.tapdata.tm.apiServer.vo.metric.OfResponseTime;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:53 Create
 * @description
 */
@Service
public class MetricResponseTime implements Metric<ApiCallMetricVo.MetricResponseTime>, InitializingBean {

    @Override
    public void fields(Query query) {
        query.fields().include("delays", "p50", "p95", "p99", "workOid", "timeStart", "processId");
    }

    @Override
    public MetricDataBase mergeTo(Long timeStart, List<WorkerCallEntity> vos) {
        OfResponseTime ofResponseTime = new OfResponseTime();
        ofResponseTime.setTime(timeStart);
        List<List<Map<Long, Integer>>> delayList = new ArrayList<>();
        vos.forEach(vo -> {
            if (vo.getDelays() != null && !vo.getDelays().isEmpty()) {
                delayList.add(ApiMetricsDelayUtil.fixDelayAsMap(vo.getDelays()));
            }
        });
        List<Map<Long, Integer>> merged = ApiMetricsDelayUtil.merge(delayList);
        BiFunction<Long, Integer, Long> f = (k, v) -> v.longValue();
        Long total = ApiMetricsDelayUtil.sum(merged, f);
        Long p95 = ApiMetricsDelayUtil.p95(merged, total);
        Long p50 = ApiMetricsDelayUtil.p50(merged, total);
        Long p99 = ApiMetricsDelayUtil.p99(merged, total);
        ofResponseTime.setP50(p50);
        ofResponseTime.setP95(p95);
        ofResponseTime.setP99(p99);
        return ofResponseTime;
    }

    @Override
    public ApiCallMetricVo.MetricResponseTime toResult(List<MetricDataBase> data) {
        ApiCallMetricVo.MetricResponseTime info = new ApiCallMetricVo.MetricResponseTime();
        data.stream()
                .filter(OfResponseTime.class::isInstance)
                .forEach(vo -> info.add(0, vo.getTime(), ((OfResponseTime) vo).getP50(), ((OfResponseTime) vo).getP95(), ((OfResponseTime) vo).getP99()));
        return info;
    }

    @Override
    public MetricDataBase mock(long time) {
        return new OfResponseTime().time(time);
    }

    @Override
    public ApiCallMetricVo.MetricBase mockMetric() {
        return new ApiCallMetricVo.MetricResponseTime();
    }

    @Override
    public void afterPropertiesSet() {
        Metric.Factory.register(Type.RESPONSE_TIME, this);
    }
}

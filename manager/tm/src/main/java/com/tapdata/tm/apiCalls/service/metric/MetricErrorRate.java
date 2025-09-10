package com.tapdata.tm.apiCalls.service.metric;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.metric.MetricDataBase;
import com.tapdata.tm.apiCalls.vo.metric.OfErrorRate;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:54 Create
 * @description
 */
@Service
public class MetricErrorRate implements Metric<ApiCallMetricVo.MetricErrorRate>, InitializingBean {

    @Override
    public void fields(Query query) {
        query.fields().include("errorRate", "errorCount", "reqCount", "workOid", "timeStart", "processId");
    }

    @Override
    public MetricDataBase mergeTo(Long timeStart, List<WorkerCallEntity> vos) {
        OfErrorRate data = new OfErrorRate();
        data.setTime(timeStart);
        long reqCount = 0L;
        long errorCount = 0L;
        for (WorkerCallEntity vo : vos) {
            reqCount += vo.getReqCount();
            errorCount += vo.getErrorCount();
        }
        data.setErrorRate(reqCount == 0L ? 0d : errorCount * 1.0d / reqCount);
        return data;
    }

    @Override
    public ApiCallMetricVo.MetricErrorRate toResult(List<MetricDataBase> data) {
        ApiCallMetricVo.MetricErrorRate info = new ApiCallMetricVo.MetricErrorRate();
        data.stream()
                .filter(OfErrorRate.class::isInstance)
                .forEach(vo -> info.add(0, vo.getTime(), ((OfErrorRate) vo).getErrorRate()));
        return info;
    }

    @Override
    public MetricDataBase mock(long time) {
        return new OfErrorRate().time(time);
    }

    @Override
    public ApiCallMetricVo.MetricBase mockMetric() {
        return new ApiCallMetricVo.MetricErrorRate();
    }

    @Override
    public void afterPropertiesSet() {
        Metric.Factory.register(Metric.Type.ERROR_RATE, this);
    }
}

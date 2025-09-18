package com.tapdata.tm.apiCalls.service.metric;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.metric.MetricDataBase;
import com.tapdata.tm.apiCalls.vo.metric.OfRPS;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 11:52 Create
 * @description
 */
@Service
public class MetricRPS implements Metric<ApiCallMetricVo.MetricRPS>, InitializingBean {

    @Override
    public void fields(Query query) {
        query.fields().include("rps", "reqCount", "workOid", "timeStart", "processId");
    }

    @Override
    public MetricDataBase mergeTo(Long timeStart, List<WorkerCallEntity> vos) {
        OfRPS ofRPS = new OfRPS();
        ofRPS.setTime(timeStart);
        double rps = 0d;
        List<WorkerCallEntity> sorted = vos.stream().filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getTimeStart()))
                .sorted(Comparator.comparing(WorkerCallEntity::getTimeStart))
                .toList();
        WorkerCallEntity last = null;
        long count = sorted.size();
        for (WorkerCallEntity vo : sorted) {
            if (last != null && (vo.getTimeStart() - last.getTimeStart()) > 60000L) {
                long diff = vo.getTimeStart() - last.getTimeStart();
                count += (diff / 60000L);
            }
            rps += Optional.ofNullable(vo.getRps()).orElse(0d) * 60d;
            last = vo;
        }
        ofRPS.setRps(count == 0L ? 0d : formatAsPercentage(rps / (count * 60d), 2, 1.0d));
        return ofRPS;
    }

    @Override
    public ApiCallMetricVo.MetricRPS toResult(List<MetricDataBase> data) {
        ApiCallMetricVo.MetricRPS info = new ApiCallMetricVo.MetricRPS();
        data.stream()
                .filter(OfRPS.class::isInstance)
                .forEach(vo -> info.add(0, vo.getTime(), ((OfRPS) vo).getRps()));
        return info;
    }

    @Override
    public MetricDataBase mock(long time) {
        return new OfRPS().time(time);
    }

    @Override
    public ApiCallMetricVo.MetricBase mockMetric() {
        return new ApiCallMetricVo.MetricRPS();
    }

    @Override
    public void afterPropertiesSet() {
        Metric.Factory.register(Metric.Type.RPS, this);
    }
}

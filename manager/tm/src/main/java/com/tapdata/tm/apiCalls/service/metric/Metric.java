package com.tapdata.tm.apiCalls.service.metric;

import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.WorkerCallData;
import com.tapdata.tm.apiCalls.vo.metric.MetricDataBase;
import lombok.Getter;
import org.springframework.data.mongodb.core.query.Query;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/3 10:44 Create
 * @description
 */
public interface Metric<T extends ApiCallMetricVo.MetricBase> {

    void fields(Query query);

    MetricDataBase mergeTo(Long timeStart, List<WorkerCallEntity> vos);

    T toResult(List<MetricDataBase> data);

    MetricDataBase mock(long time);

    static Metric<? extends ApiCallMetricVo.MetricBase> call(int type) {
        return Factory.get(Type.by(type));
    }

    class Factory {
        private Factory() {}
        private static final Map<Type, Metric<? extends ApiCallMetricVo.MetricBase>> instances = new EnumMap<>(Type.class);

        public static Metric<? extends ApiCallMetricVo.MetricBase> get(Type type) {
            return instances.get(type);
        }

        public static void register(Type type, Metric<? extends ApiCallMetricVo.MetricBase> metric) {
            instances.put(type, metric);
        }
    }

    @Getter
    public enum Type {
        RPS(0), RESPONSE_TIME(1), ERROR_RATE(2);
        final int code;

        Type(int code) {
            this.code = code;
        }

        public static Type by(int code) {
            for (Type value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            return RPS;
        }
    }
}

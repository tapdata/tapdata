package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class ApiRequestTrend extends ValueBase {
    private List<Long> ts = new ArrayList<>();
    private List<Double> values = new ArrayList<>();

    public static ApiRequestTrend create(TimeGranularity granularity) {
        ApiRequestTrend trend = new ApiRequestTrend();
        trend.setGranularity(granularity.getType());
        return trend;
    }

    public void add(Item item) {
        ts.add(item.getTs());
        values.add(item.getValue());
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Item extends ValueBase.Item {
        private double value;

        public static Item create(long ts) {
            Item item = new Item();
            item.setTs(ts);
            item.setValue(0D);
            return item;
        }
    }
}

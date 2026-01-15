package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:11 Create
 * @description
 *
 * 5S 攒五分钟
 * 1minute 攒1小时
 * 1hour 直接用点位
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ChartAndDelayOfApi extends ValueBase {
    List<Long> ts;
    @DecimalFormat
    List<Double> rps;
    List<Long> p95;
    List<Long> p99;
    List<Long> maxDelay;
    List<Long> minDelay;
    @DecimalFormat
    List<Double> requestCostAvg;

    public static ChartAndDelayOfApi create() {
        ChartAndDelayOfApi item = new ChartAndDelayOfApi();
        item.setTs(new ArrayList<>());
        item.setRps(new ArrayList<>());
        item.setP95(new ArrayList<>());
        item.setP99(new ArrayList<>());
        item.setMaxDelay(new ArrayList<>());
        item.setMinDelay(new ArrayList<>());
        item.setRequestCostAvg(new ArrayList<>());
        return item;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Item extends ValueBase.Item {
        Double rps;
        Long p95;
        Long p99;
        Long maxDelay;
        Long minDelay;
        Double requestCostAvg;
        boolean tag;

        Long totalBytes;
        List<Map<Long, Integer>> delay = new ArrayList<>();

        public static Item create(long ts) {
            Item item = new Item();
            item.setTs(ts);
            item.setTotalBytes(0L);
            item.setTag(true);
            return item;
        }
    }
}

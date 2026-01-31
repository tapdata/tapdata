package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.commons.base.DecimalFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:11 Create
 * @description 5S 攒五分钟
 * 1minute 攒1小时
 * 1hour 直接用点位
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ChartAndDelayOfApi extends ValueBase {
    List<Long> ts;
    @DecimalFormat
    List<Double> rps;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> p95;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> p99;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> maxDelay;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> minDelay;
    @DecimalFormat
    List<Double> requestCostAvg;

    @DecimalFormat
    List<Double> dbCostAvg;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> dbCostMax;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> dbCostMin;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> dbCostP95;
    @DecimalFormat(scale = 1, maxScale = 1)
    List<Double> dbCostP99;

    public static ChartAndDelayOfApi create() {
        ChartAndDelayOfApi item = new ChartAndDelayOfApi();
        item.setTs(new ArrayList<>());
        item.setRps(new ArrayList<>());
        item.setP95(new ArrayList<>());
        item.setP99(new ArrayList<>());
        item.setMaxDelay(new ArrayList<>());
        item.setMinDelay(new ArrayList<>());
        item.setRequestCostAvg(new ArrayList<>());
        item.setDbCostAvg(new ArrayList<>());
        item.setDbCostMax(new ArrayList<>());
        item.setDbCostMin(new ArrayList<>());
        item.setDbCostP95(new ArrayList<>());
        item.setDbCostP99(new ArrayList<>());
        return item;
    }

    public void add(Item item) {
        getTs().add(item.getTs());
        if (!item.isTag()) {
            getMaxDelay().add(item.getMaxDelay());
            getMinDelay().add(item.getMinDelay());
            getRps().add(item.getRps());
            getRequestCostAvg().add(item.getRequestCostAvg());
            getDbCostAvg().add(item.getDbCostAvg());
            getDbCostMin().add(item.getDbCostMin());
            getDbCostMax().add(item.getDbCostMax());
        } else {
            getMaxDelay().add(null);
            getMinDelay().add(null);
            getRps().add(0D);
            getRequestCostAvg().add(0D);
            getDbCostAvg().add(0D);
            getDbCostMin().add(null);
            getDbCostMax().add(null);
        }
        getDbCostP95().add(item.getDbCostP95());
        getDbCostP99().add(item.getDbCostP99());
        getP95().add(item.getP95());
        getP99().add(item.getP99());
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Item extends ValueBase.Item {
        Double rps;
        Double p95;
        Double p99;
        Double maxDelay;
        Double minDelay;
        Double requestCostAvg;
        boolean tag;

        double dbCostAvg;
        Double dbCostMax;
        Double dbCostMin;
        Double dbCostP95;
        Double dbCostP99;

        Long totalBytes;
        List<Map<String, Number>> delay = new ArrayList<>();
        List<Map<String, Number>> dbCost = new ArrayList<>();

        List<List<Map<String, Number>>> delays;
        List<Long> bytes;
        List<List<Map<String, Number>>> dbCosts;

        public static Item create(long ts) {
            Item item = new Item();
            item.setTs(ts);
            item.setTotalBytes(0L);
            item.setTag(true);
            return item;
        }

        public void point(List<List<Map<String, Number>>> delays, List<Long> bytes, List<List<Map<String, Number>>> dbCosts) {
            Optional.ofNullable(delays).map(e -> new ArrayList(e)).ifPresent(this::setDelays);
            Optional.ofNullable(bytes).map(e -> new ArrayList(e)).ifPresent(this::setBytes);
            Optional.ofNullable(dbCosts).map(e -> new ArrayList(e)).ifPresent(this::setDbCosts);
        }
    }
}

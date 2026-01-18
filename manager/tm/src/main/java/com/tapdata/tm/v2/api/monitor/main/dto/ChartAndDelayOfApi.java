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
    List<Long> p95;
    List<Long> p99;
    List<Long> maxDelay;
    List<Long> minDelay;
    @DecimalFormat
    List<Double> requestCostAvg;

    @DecimalFormat
    List<Double> dbCostAvg;
    List<Long> dbCostMax;
    List<Long> dbCostMin;
    List<Long> dbCostP95;
    List<Long> dbCostP99;

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
        Long p95;
        Long p99;
        Long maxDelay;
        Long minDelay;
        Double requestCostAvg;
        boolean tag;

        double dbCostAvg;
        Long dbCostMax;
        Long dbCostMin;
        Long dbCostP95;
        Long dbCostP99;

        Long totalBytes;
        List<Map<Long, Integer>> delay = new ArrayList<>();
        List<Map<Long, Integer>> dbCost = new ArrayList<>();

        public static Item create(long ts) {
            Item item = new Item();
            item.setTs(ts);
            item.setTotalBytes(0L);
            item.setTag(true);
            return item;
        }
    }
}

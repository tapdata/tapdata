package io.tapdata.common.sample.request;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Dexter
 */
public class BulkStatisticRequest {
    List<Statistic> statistics = new ArrayList<>();

    public List<Statistic> getStatistics() {
        return statistics;
    }

    public void setStatistics(List<Statistic> statistics) {
        this.statistics = statistics;
    }
}

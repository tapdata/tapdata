package io.tapdata.common.sample.request;

import java.util.Map;

/**
 * @author Dexter
 */
public class StatisticRequest {
    private Map<String, String> tags;
    private Statistic statistic;

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Statistic getStatistic() {
        return statistic;
    }

    public void setStatistic(Statistic statistic) {
        this.statistic = statistic;
    }

    @Override
    public String toString() {
        return "StatisticRequest{" +
                "tags=" + tags +
                ", statistic=" + statistic +
                '}';
    }
}

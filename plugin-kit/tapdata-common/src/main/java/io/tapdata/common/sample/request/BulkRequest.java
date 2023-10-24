package io.tapdata.common.sample.request;

import java.util.ArrayList;
import java.util.List;


public class BulkRequest {
    List<SampleRequest> samples = new ArrayList<>();
    List<StatisticRequest> statistics = new ArrayList<>();

    public List<SampleRequest> getSamples() {
        return samples;
    }

    public void setSamples(List<SampleRequest> samples) {
        this.samples = samples;
    }

    public List<StatisticRequest> getStatistics() {
        return statistics;
    }

    public void setStatistics(List<StatisticRequest> statistics) {
        this.statistics = statistics;
    }

    public void addSampleRequest(SampleRequest sampleRequest) {
        samples.add(sampleRequest);
    }

    public void addStatisticRequest(StatisticRequest statisticRequest) {
        statistics.add(statisticRequest);
    }

    @Override
    public String toString() {
        return "BulkRequest{" +
                "samples=" + samples +
                ", statistics=" + statistics +
                '}';
    }
}

package com.tapdata.tm.monitor.param;

import io.tapdata.common.sample.request.Sample;

import java.util.List;
import java.util.Map;

public class AgentEnvironmentParam {
    private Map tags;
    private List<Sample> sampleList;

    public Map getTags() {
        return tags;
    }

    public void setTags(Map tags) {
        this.tags = tags;
    }

    public List getSampleList() {
        return sampleList;
    }

    public void setSampleList(List sampleList) {
        this.sampleList = sampleList;
    }
}

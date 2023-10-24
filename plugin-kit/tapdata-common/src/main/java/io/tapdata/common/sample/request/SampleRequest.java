package io.tapdata.common.sample.request;

import java.util.Map;

public class SampleRequest {
    private Map<String, String> tags;
    private Sample sample;

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Sample getSample() {
        return sample;
    }

    public void setSample(Sample sample) {
        this.sample = sample;
    }

    @Override
    public String toString() {
        return "SampleRequest{" +
                "tags=" + tags +
                ", sample=" + sample +
                '}';
    }
}

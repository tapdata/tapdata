package io.tapdata.common.sample.request;

import java.util.ArrayList;
import java.util.List;

public class BulkSampleRequest {
    List<SampleRequest> samples = new ArrayList<>();

    public void setSamples(List<SampleRequest> samples) {
        this.samples = samples;
    }

    public List<SampleRequest> getSampleRequests() {
        return samples;
    }
}

package io.tapdata.bigquery.service.stream.stage;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface WriteModeHandel {
    public static final Map<String, Class<? extends WriteModeHandel>> writeCollection = new HashMap<String, Class<? extends WriteModeHandel>>() {{
        put("APPEND_ONLY", AppendOnlyMode.class);
        put("MIXED_UPDATES", MixedAndAppendMode.class);
    }};

    public static WriteModeHandel write(String modeType) {
        Class<? extends WriteModeHandel> cla = WriteModeHandel.writeCollection.get(modeType);
        try {
            return cla.newInstance();
        } catch (Exception e) {
            throw new CoreException(String.format("Unable to get WriteModeHandel's sub class object according to type %s, %s is not supported temporarily. ", modeType, modeType));
        }
    }

    public void append(List<Map<String, Object>> records, WriteListResult<TapRecordEvent> result);
}

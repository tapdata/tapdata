package io.tapdata.bigquery.service.stream.stage;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.Map;

public class MixedAndAppendMode implements WriteModeHandel{
    @Override
    public void append(List<Map<String, Object>> records, WriteListResult<TapRecordEvent> result) {

    }
}

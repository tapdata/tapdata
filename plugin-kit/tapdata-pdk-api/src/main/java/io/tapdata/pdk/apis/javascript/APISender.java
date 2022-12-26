package io.tapdata.pdk.apis.javascript;

import java.util.List;
import java.util.Map;

public interface APISender {
    public void send(List<Object> data, boolean hasNext, Object offsetState);
}

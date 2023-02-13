package io.tapdata.common.support;

import java.util.List;

public interface APISender {
    public void send(Object data, String tableName);

    public void send(Object data, String tableName, String eventType);

    public default void send(Object data, String tableName, boolean cacheAgoRecord) {
        this.send(data, tableName);
    }

    public List<Object> covertList(Object obj, String tableName);
}

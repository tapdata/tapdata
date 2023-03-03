package io.tapdata.common.support;

import java.util.List;

public interface APISender {
    public void send(Object data, String tableName, Object offset);

    public void send(Object data, String tableName, String eventType, Object offset);

    public default void send(Object data, String tableName, Object offset, boolean cacheAgoRecord) {
        this.send(data, tableName,offset);
    }
    public void send(Object offset);

    public List<Object> covertList(Object obj, String tableName);
}

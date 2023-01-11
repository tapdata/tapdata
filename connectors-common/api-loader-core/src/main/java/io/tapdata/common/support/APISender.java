package io.tapdata.common.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface APISender {
    public void send(Object data, Object offsetState);

    public default void send(Object data, Object offsetState, boolean cacheAgoRecord) {
        this.send(data, offsetState);
    }

    public default List<Object> covertList(Object obj){
        List<Object> list = new ArrayList<>();
        if (obj instanceof Collection) {
            list.addAll((Collection<Object>) obj);
            return list;
        }
        list.add(obj);
        return list;
    }
}

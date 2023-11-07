package io.tapdata.flow.engine.V2.filter;

import com.tapdata.entity.TapdataEvent;

public interface TapEventFilter<T, V> {

    public void addHandler(T tapEventPredicate);

    public <E extends TapdataEvent> V handle(E event);
}

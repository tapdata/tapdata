package io.tapdata.flow.engine.V2.filter;

import com.tapdata.entity.TapdataEvent;

public interface TapEventFilter<T, V> {

    public void addFilter(T tapEventPredicate);

    public <E extends TapdataEvent> V test(E event);
}

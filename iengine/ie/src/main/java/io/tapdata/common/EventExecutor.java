package io.tapdata.common;

import com.tapdata.entity.Event;

public interface EventExecutor {

	Event execute(Event event);
}

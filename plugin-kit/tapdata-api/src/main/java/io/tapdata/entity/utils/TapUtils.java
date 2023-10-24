package io.tapdata.entity.utils;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;

public interface TapUtils {
    ScheduledFuture<?> interval(Runnable runnable, int seconds);
    Map<String, Object> cloneMap(Map<String, Object> map);

    String getStackTrace(Throwable throwable);
}

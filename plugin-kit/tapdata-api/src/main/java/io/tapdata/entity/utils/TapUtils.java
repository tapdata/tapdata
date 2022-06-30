package io.tapdata.entity.utils;

import java.util.Map;

public interface TapUtils {
    void interval(Runnable runnable, int seconds);
    Map<String, Object> cloneMap(Map<String, Object> map);
}

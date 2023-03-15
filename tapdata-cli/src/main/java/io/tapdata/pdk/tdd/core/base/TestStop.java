package io.tapdata.pdk.tdd.core.base;

import java.lang.reflect.Method;

public interface TestStop {
    public void stop(TestNode prepare, Method testCase);
}
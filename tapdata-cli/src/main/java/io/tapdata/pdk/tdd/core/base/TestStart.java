package io.tapdata.pdk.tdd.core.base;

import java.lang.reflect.Method;

public interface TestStart {
    public void start(TestNode prepare, Method testCase);
}
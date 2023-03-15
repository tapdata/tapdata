package io.tapdata.pdk.tdd.core.base;

import java.lang.reflect.Method;

public interface TestExec {
    public void exec(TestNode prepare, Method testCase);
}
package io.tapdata.pdk.tdd.tests.support;

import java.lang.reflect.Method;

public class TapAssertImpl implements TapAssert {

    int grade;
    public TapAssertImpl(int grade){
        this.grade = grade;
    }

    public void accept(Method testCase, String msg) {
        accept(testCase,grade,msg);
    }

    @Override
    public void consumer() {

    }
}

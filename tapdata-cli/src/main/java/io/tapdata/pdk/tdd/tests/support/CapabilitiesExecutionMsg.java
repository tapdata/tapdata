package io.tapdata.pdk.tdd.tests.support;


import org.junit.jupiter.api.DisplayName;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

public class CapabilitiesExecutionMsg {
    public static final int ERROR = 0;
    public static final int SUCCEED = 1;
    public static final int WARN = 2;

    String executionMsg;
    Map<Method, Case> testCase = new TreeMap<>((method1, method2) -> {
        Annotation annotation1 = method1.getAnnotation(TapTestCase.class);
        Annotation annotation2 = method2.getAnnotation(TapTestCase.class);
        if (null == annotation2 || null == annotation1) return 1;
        return Integer.compare(((TapTestCase) annotation1).sort(), ((TapTestCase) annotation2).sort());
    });
    int executionResult = SUCCEED;

    public static CapabilitiesExecutionMsg create() {
        return new CapabilitiesExecutionMsg();
    }

    public CapabilitiesExecutionMsg executionMsg(String executionMsg) {
        this.executionMsg = executionMsg;
        return this;
    }

    //    public CapabilitiesExecutionMsg addCase(Method caseMethod,Case testCase){
//        if (null != testCase) {
//            this.testCase.put(caseMethod,testCase);
//        }
//        return this;
//    }
    public CapabilitiesExecutionMsg clean() {
        this.testCase = new HashMap<>();
        this.executionMsg = "";
        return this;
    }

    public CapabilitiesExecutionMsg fail() {
        if (ERROR != this.executionResult) this.executionResult = ERROR;
        return this;
    }

    public CapabilitiesExecutionMsg warn() {
        if (WARN != this.executionResult) this.executionResult = WARN;
        return this;
    }

    public int executionResult() {
        return this.executionResult;
    }

    public Map<Method, Case> testCases() {
        return this.testCase;
    }

    public Case testCase(Method testCase) {
        DisplayName annotation = testCase.getAnnotation(DisplayName.class);
        String value = annotation.value();
        return this.testCase.computeIfAbsent(
                testCase,
                testCaseMethod -> null == this.testCase.get(testCaseMethod) ?
                        new Case(Case.SUCCEED, value) : this.testCase.get(testCase)
        );
    }

    public Map<String, List<Map<Method, Case>>> testCaseGroupTag() {
        Map<String, List<Map<Method, Case>>> group = new HashMap<>();
        this.testCase.forEach((method, test) -> {
            List<Map<Method, Case>> maps = group.computeIfAbsent(test.tag, tags -> null == group.get(tags) ? new ArrayList<>() : group.get(tags));
            HashMap<Method, Case> objectObjectHashMap = new HashMap<>();
            objectObjectHashMap.put(method, test);
            maps.add(objectObjectHashMap);
        });
        return group;
    }
}

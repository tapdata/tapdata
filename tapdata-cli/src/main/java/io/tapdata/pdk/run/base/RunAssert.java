package io.tapdata.pdk.run.base;

import io.tapdata.pdk.tdd.tests.support.CapabilitiesExecutionMsg;
import io.tapdata.pdk.tdd.tests.support.Case;
import org.opentest4j.AssertionFailedError;

import java.lang.reflect.Method;

public interface RunAssert {
    public static void change(Throwable e, String message, int assertGarde, Method testCase) {
        Class<?> declaringClass = testCase.getDeclaringClass();

        CapabilitiesExecutionMsg msg = RunnerSummary.capabilitiesResult.computeIfAbsent(
                declaringClass,
                cls -> null == RunnerSummary.capabilitiesResult.get(declaringClass) ?
                        new CapabilitiesExecutionMsg()
                        : RunnerSummary.capabilitiesResult.get(cls)
        );
        Case testCases = msg.testCase(testCase);
        switch (assertGarde) {
            case WARN: {
                //TapLogger.warn("TAP-TEST",message);
                if (msg.executionResult() == SUCCEED) {
                    msg.warn();
                }
                testCases.addWarn(message);
                break;
            }
            case ERROR: {
                //TapLogger.error("TAP-TEST",message);
                if (ERROR != msg.executionResult()) {
                    msg.fail();
                }
                testCases.addError(message);
                throw new RuntimeException(message, e);
            }
            case SUCCEED: {
                if (null != message && !"".equals(message)) {
                    testCases.addSucceed(message);
                }
                //TapLogger.info("TAP-TEST",message);
            }
        }
    }

    static final int ERROR = 0;//default
    static final int WARN = 1;
    static final int SUCCEED = 2;

    public default void acceptAsWarn(Method testCase, String succeedMag) {
        accept(testCase, WARN, succeedMag);
    }

    public default void warn(Method testCase) {
        accept(testCase, WARN, null);
    }

    public default void acceptAsError(Method testCase, String succeedMag) {
        accept(testCase, ERROR, succeedMag);
    }

    public default void error(Method testCase) {
        accept(testCase, ERROR, null);
    }

    public static void succeed(Method testCase, String message) {
        RunAssert.asserts(() -> {
        }).accept(testCase, SUCCEED, message);
    }

    public static void error(Method testCase, String message) {
        RunAssert.asserts(() -> {
            throw new RuntimeException(message);
        }).accept(testCase, ERROR, message);
    }

    public static void warn(Method testCase, String message) {
        RunAssert.asserts(() -> {
            throw new RuntimeException(message);
        }).accept(testCase, WARN, message);
    }

    public default void accept(Method testCase, int assertGarde, String succeedMag) {
        try {
            consumer();
        } catch (AssertionFailedError e) {
            String message = e.getMessage();
            if (message.contains("==> expected:")) {
                message = message.substring(0, message.indexOf("==> expected:"));
            } else if (message.contains("==> Unexpected exception thrown:")) {
                message = message.substring(0, message.indexOf("==> Unexpected exception thrown:"));
            }
            change(e, message, assertGarde, testCase);
            return;
        } catch (Exception e) {
            change(e, e.getMessage(), assertGarde, testCase);
            return;
        }
        change(null, succeedMag, SUCCEED, testCase);
    }

    public void consumer();

    public static RunAssert asserts(RunAssert tapAssert) {
        return tapAssert;
    }
}

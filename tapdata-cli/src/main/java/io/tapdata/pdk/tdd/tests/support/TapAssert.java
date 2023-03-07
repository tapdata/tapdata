package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.pdk.tdd.core.base.TapAssertException;
import org.opentest4j.AssertionFailedError;

import java.lang.reflect.Method;

public interface TapAssert {
    static final int ERROR = 0;//default
    static final int WARN = 1;
    static final int SUCCEED = 2;
    public static void change(Throwable e, String message, int assertGarde, Method testCase){
        change(e, message, assertGarde, testCase,true);
    }
    public static void change(Throwable e, String message, int assertGarde, Method testCase,boolean throwIs) {
        Class<?> declaringClass = testCase.getDeclaringClass();
        CapabilitiesExecutionMsg msg = null;
        synchronized (TapAssert.class){
           msg = TapSummary.capabilitiesResult.computeIfAbsent(
                   declaringClass,
                   cls -> null == TapSummary.capabilitiesResult.get(declaringClass) ?
                           new CapabilitiesExecutionMsg()
                           : TapSummary.capabilitiesResult.get(cls)
           );
       }
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
                if (throwIs) {
                    throw new TapAssertException(message, e);
                }
            }
            case SUCCEED: {
                if (null != message && !"".equals(message)) {
                    testCases.addSucceed(message);
                }
                //TapLogger.info("TAP-TEST",message);
            }
        }
    }

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
        TapAssert.asserts(() -> {
        }).accept(testCase, SUCCEED, message);
    }

    public static void error(Method testCase, String message) {
        TapAssert.asserts(() -> {
            throw new AssertionFailedError(message);
        }).accept(testCase, ERROR, message);
    }
    public static void errorNotThrow(Method testCase, String message) {
        TapAssert.asserts(() -> {
            throw new AssertionFailedError(message);
        }).accept(testCase, ERROR, message,false);
    }

    public static void warn(Method testCase, String message) {
        TapAssert.asserts(() -> {
            throw new AssertionFailedError(message);
        }).accept(testCase, WARN, message);
    }
    public default void accept(Method testCase, int assertGarde, String succeedMag){
        accept(testCase,assertGarde,succeedMag,true);
    }
    public default void accept(Method testCase, int assertGarde, String succeedMag, boolean isThrow) {
        try {
            consumer();
        } catch (AssertionFailedError e) {
            String message = e.getMessage();
            if (message.contains("==> expected:")) {
                message = message.substring(0, message.indexOf("==> expected:"));
            } else if (message.contains("==> Unexpected exception thrown:")) {
                message = message.substring(0, message.indexOf("==> Unexpected exception thrown:"));
            }
            change(e, message, assertGarde, testCase,isThrow);
            return;
        } catch (Exception e) {
            change(e, e.getMessage(), assertGarde, testCase,isThrow);
            throw e;
        }
        change(null, succeedMag, SUCCEED, testCase,isThrow);
    }

    public void consumer();

    public static TapAssert asserts(TapAssert tapAssert) {
        return tapAssert;
    }
}

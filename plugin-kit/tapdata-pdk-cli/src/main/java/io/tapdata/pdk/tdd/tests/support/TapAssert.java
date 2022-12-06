package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.pdk.cli.commands.TDDCli;
import io.tapdata.pdk.cli.commands.TapSummary;
import org.junit.jupiter.api.DisplayName;
import org.opentest4j.AssertionFailedError;

import java.lang.reflect.Method;
import java.util.List;

public interface TapAssert {
    static final int ERROR = 0;//default
    static final int WARN = 1;
    static final int SUCCEED = 2;

    public static void change(Throwable e, String message, int assertGarde,Method testCase) {
        Class<?> declaringClass = testCase.getDeclaringClass();

        CapabilitiesExecutionMsg msg = TapSummary.capabilitiesResult.computeIfAbsent(
                declaringClass,
                cls -> null == TapSummary.capabilitiesResult.get(declaringClass) ?
                                new CapabilitiesExecutionMsg()
                                : TapSummary.capabilitiesResult.get(cls)
        );
        Case testCases = msg.testCase(testCase);
        switch (assertGarde){
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
                throw new RuntimeException(message,e);
            }
            case SUCCEED: {
                if (null != message && !"".equals(message)) {
                    testCases.addSucceed(message);
                }
                //TapLogger.info("TAP-TEST",message);
            }
        }
    }
    public default void acceptAsWarn(Method testCase,String succeedMag){
        accept(testCase,WARN,succeedMag);
    }
    public default void warn(Method testCase){
        accept(testCase,WARN,null);
    }
    public default void acceptAsError(Method testCase,String succeedMag){
        accept(testCase,ERROR,succeedMag);
    }
    public default void error(Method testCase){
        accept(testCase,ERROR,null);
    }
    public static void succeed(Method testCase,String message){
        TapAssert.asserts(()->{}).accept(testCase,SUCCEED,message);
    }

    public default void accept(Method testCase,int assertGarde,String succeedMag){
        try {
            consumer();
        }catch (AssertionFailedError e){
            String message = e.getMessage();
            if (message.contains("==> expected:")){
                message = message.substring(0,message.indexOf("==> expected:"));
            }else if (message.contains("==> Unexpected exception thrown:")){
                message = message.substring(0,message.indexOf("==> Unexpected exception thrown:"));
            }
            change(e,message,assertGarde,testCase);
            return;
        }catch (Exception e){
            change(e,e.getMessage(),assertGarde,testCase);
            throw e;
        }
        change(null,succeedMag,SUCCEED,testCase);
    }

    public void consumer();

    public static TapAssert asserts(TapAssert tapAssert){
        return tapAssert;
    }
}

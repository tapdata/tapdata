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

    public static void change(Throwable e, String message, int assertGarde, Class target, Method testCase) {
        CapabilitiesExecutionMsg msg = TapSummary.capabilitiesResult.computeIfAbsent(
                target,
                cls -> null == TapSummary.capabilitiesResult.get(cls) ?
                                new CapabilitiesExecutionMsg()
                                : TapSummary.capabilitiesResult.get(cls)
        );
        Case testCases = msg.testCase(testCase);
        switch (assertGarde){
            case WARN: {
                //TapLogger.warn("TAP-TEST",message);
                msg.warn();
                testCases.addWarn(message);
                break;
            }
            case ERROR: {
                //TapLogger.error("TAP-TEST",message);
                msg.fail();
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
    public default void acceptAsWarn(Class target,Method testCase,String succeedMag){
        accept(target,testCase,WARN,succeedMag);
    }
    public default void acceptAsError(Class target,Method testCase,String succeedMag){
        accept(target,testCase,ERROR,succeedMag);
    }


    public default void accept(Class target,Method testCase,int assertGarde,String succeedMag){
        try {
            consumer();
        }catch (AssertionFailedError e){
            String message = e.getMessage();
            if (message.contains("==> expected:")){
                message = message.substring(0,message.indexOf("==> expected:"));
            }
            change(e,message,assertGarde,target,testCase);
            return;
        }catch (Exception e){
            change(e,e.getMessage(),assertGarde,target,testCase);
            throw e;
        }
        change(null,succeedMag,SUCCEED,target,testCase);
    }

    public void consumer();

    public static TapAssert asserts(TapAssert tapAssert){
        return tapAssert;
    }
}

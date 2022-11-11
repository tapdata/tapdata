package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.commands.TDDCli;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface TapAssert {
    static final int ERROR = 0;//default
    static final int WARN = 1;
    static final int SUCCEED = 2;

    public static void change(Throwable e,String message,int assertGarde,Class target) {
        CapabilitiesExecutionMsg msg = TDDCli.TapSummary.capabilitiesResult.computeIfAbsent(target, cls -> null == TDDCli.TapSummary.capabilitiesResult.get(cls) ? new CapabilitiesExecutionMsg() : TDDCli.TapSummary.capabilitiesResult.get(cls));
        List<History> histories = msg.history();
        msg.addTimes(1);
        switch (assertGarde){
            case WARN: {
                //TapLogger.warn("TAP-TEST",message);
                histories.add(History.warn(message));
            }
            case ERROR: {
                //TapLogger.error("TAP-TEST",message);
                msg.fail();
                histories.add(History.error(message));
                throw new RuntimeException(message,e);
            }
            case SUCCEED: {
                if (null != message && !"".equals(message)) {
                    histories.add(History.succeed(message));
                }
                //TapLogger.info("TAP-TEST",message);
            }
        }
    }
    public default void acceptAsWarn(Class target,String succeedMag){
        accept(target,WARN,succeedMag);
    }
    public default void acceptAsError(Class target,String succeedMag){
        accept(target,ERROR,succeedMag);
    }


    public default void accept(Class target,int assertGarde,String succeedMag){
        try {
            consumer();
        }catch (AssertionFailedError e){
            change(e,e.getMessage(),assertGarde,target);
            return;
        }catch (Exception e){
            change(e,e.getMessage(),assertGarde,target);
            throw e;
        }
        change(null,succeedMag,SUCCEED,target);
    }
    public void consumer();

    public static TapAssert asserts(TapAssert tapAssert){
        return tapAssert;
    }
}

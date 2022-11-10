package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.logger.TapLogger;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import java.util.HashMap;
import java.util.Map;

public class TapAssert extends Assertions{
    protected static final int ERROR = 0;//default
    protected static final int WARN = 1;
    protected static final int SUCCEED = 2;

    public Integer assertGarde = ERROR;
    private Map<Class,CapabilitiesExecutionMsg> capabilitiesMap ;
    private Class capabilitiesClass;

    public static TapAssert defaultAssert(){
        return new TapAssert(ERROR);
    }
    public static TapAssert warnAssert(){
        return new TapAssert(WARN);
    }
    public TapAssert capabilities(Map<Class,CapabilitiesExecutionMsg> capabilitiesMap,Class capabilitiesClass){
        this.capabilitiesClass = capabilitiesClass;
        this.capabilitiesMap = capabilitiesMap;
        return this;
    }
    public static TapAssert succeedAssert(){
        return new TapAssert(SUCCEED);
    }

    protected TapAssert(int assertGrade){
        super();
        assertGarde = assertGrade;
    }

    public void assertNotNulls(Object actual, String message){
        CapabilitiesExecutionMsg msg = capabilitiesMap.computeIfAbsent(capabilitiesClass,cap->null==capabilitiesMap.get(cap)?CapabilitiesExecutionMsg.create():capabilitiesMap.get(cap));
        msg.addTimes();
        Map<String,Object> history = new HashMap<>();
        msg.addHistory(history);
        try {
            Assertions.assertNotNull(actual,message);
            history.put("SUCCEED",message);
        }catch (AssertionFailedError e){
            switch (this.assertGarde){
                case WARN: {
                    TapLogger.warn("TAP-TEST",message);
                    history.put("WARN",message);
                }
                case ERROR: {
                    TapLogger.error("TAP-TEST",message);
                    history.put("ERROR",message);
                    msg.fail();
                    throw new RuntimeException(message,e);
                }
                case SUCCEED: {
                    TapLogger.info("TAP-TEST",message);
                    history.put("SUCCEED",message);
                }
            }
        }
    }


//    public static <V>V fail(String message) {
//        try {
//            return Assertions.fail(message);
//        }catch (AssertionFailedError e){
//            change(e,message);
//            return null;
//        }catch (Exception e){
//            throw e;
//        }
//    }
//
//
//    public static <V>V fail(String message, Throwable cause) {
//        try {
//            return Assertions.fail(message, cause);
//        }catch (AssertionFailedError e){
//            change(e,message);
//            return null;
//        }catch (Exception e){
//            throw e;
//        }
//    }

    public static void change(Throwable e,String message,int assertGarde) {
        switch (assertGarde){
            case WARN: {
                TapLogger.warn("TAP-TEST",message);
            }
            case ERROR: {
                TapLogger.error("TAP-TEST",message);
                throw new RuntimeException(message,e);
            }
            case SUCCEED: {
                TapLogger.info("TAP-TEST",message);
            }
        }
    }
}

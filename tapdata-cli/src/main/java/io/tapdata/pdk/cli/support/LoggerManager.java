package io.tapdata.pdk.cli.support;

//import ch.qos.logback.classic.Level;
//import ch.qos.logback.classic.Logger;
//import ch.qos.logback.classic.LoggerContext;
//import ch.qos.logback.classic.turbo.TurboFilter;
//import ch.qos.logback.core.spi.FilterReply;
import io.tapdata.pdk.cli.commands.TDDCli;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.slf4j.Marker;
import org.slf4j.impl.StaticLoggerBinder;

import java.lang.reflect.Field;

public class LoggerManager {
    private static final String TAG = LoggerManager.class.getSimpleName();

    public static void changeLogLeave(LogLeave leave){
//        CommonUtils.ignoreAnyError(() -> {
//            StaticLoggerBinder staticLoggerBinder = StaticLoggerBinder.getSingleton();
//            Field field = staticLoggerBinder.getClass().getDeclaredField("defaultLoggerContext");
//            field.setAccessible(true);
//            LoggerContext loggerContext = (LoggerContext) field.get(staticLoggerBinder);
//            loggerContext.addTurboFilter(new TurboFilter() {
//                @Override
//                public FilterReply decide(Marker marker, Logger logger, Level level, String s, Object[] objects, Throwable throwable) {
//                    return leave.leave();
//                }
//            });
//        }, TAG);
    }

    public enum LogLeave{
//        DENY(FilterReply.DENY),
//        NEUTRAL(FilterReply.NEUTRAL),
//        ACCEPT(FilterReply.ACCEPT);
//        FilterReply leave;
//        LogLeave(FilterReply leave){
//            this.leave = leave;
//        }
//        public FilterReply leave(){
//            return this.leave;
//        }
    }
}

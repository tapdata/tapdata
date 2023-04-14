package io.tapdata.task.skiperrorevent;

import io.tapdata.aspect.LoggerInitAspect;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.MarkerFilter;
import org.apache.logging.log4j.core.layout.PatternLayout;

@AspectObserverClass(LoggerInitAspect.class)
public class SkipErrorEventAppenderAspectHandle implements AspectObserver<LoggerInitAspect> {

    public final static String SKIP_ERROR_EVENT_APPENDER_NAME = "skipErrorEventAppender";
    public final static String SKIP_ERROR_EVENT_MARKER = "SKIP_ERROR_EVENT";

    @Override
    public void observe(LoggerInitAspect aspect) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        LoggerConfig rootLoggerConfig = config.getRootLogger();

        // 其它 appender 过滤日志
        MarkerFilter ignoreSkipEventFilter = MarkerFilter.createFilter(SKIP_ERROR_EVENT_MARKER, Filter.Result.DENY, Filter.Result.NEUTRAL);
        rootLoggerConfig.getAppenders().forEach((s, appender) -> {
            if (appender instanceof ConsoleAppender) {
                ((ConsoleAppender) appender).addFilter(ignoreSkipEventFilter);
            } else if (appender instanceof FileAppender) {
                ((FileAppender) appender).addFilter(ignoreSkipEventFilter);
            }
        });

        TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
        SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy("1GB");
        CompositeTriggeringPolicy compositeTriggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy, sizeBasedTriggeringPolicy);
        DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder()
                .withMax("30")
                .withConfig(config).build();

        RollingFileAppender skipTrace = RollingFileAppender.newBuilder()
                .setName(SKIP_ERROR_EVENT_APPENDER_NAME)
                .withFileName(aspect.getLogsPath() + "/skip-error-event.log")
                .withFilePattern(aspect.getLogsPath() + "/skip-trace.log.%d{yyyyMMdd}.gz")
                .setLayout(PatternLayout.newBuilder()
                        .withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} %msg%n")
                        .build())
                .withPolicy(compositeTriggeringPolicy)
                .withStrategy(strategy)
                .build();
        config.addAppender(skipTrace);
        config.addLogger(SkipErrorEventAspectTask.class.getName(), LoggerConfig.createLogger(
                true, aspect.getDefaultLogLevel(), SkipErrorEventAspectTask.class.getName(), "TRUE", new AppenderRef[]{
                        AppenderRef.createAppenderRef(SKIP_ERROR_EVENT_APPENDER_NAME, null, null)
                }, null, config, null
        ));

        rootLoggerConfig.addAppender(skipTrace, aspect.getDefaultLogLevel(), MarkerFilter.createFilter(SKIP_ERROR_EVENT_MARKER, Filter.Result.ACCEPT, Filter.Result.DENY));

        skipTrace.start();
        ctx.updateLoggers();
    }
}

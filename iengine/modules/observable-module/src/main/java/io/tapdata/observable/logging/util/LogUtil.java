package io.tapdata.observable.logging.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.action.*;
import org.apache.logging.log4j.core.config.Configuration;

public class LogUtil {
    public static CompositeTriggeringPolicy getCompositeTriggeringPolicy(String logFileSaveSize) {
        TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
        SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(logFileSaveSize + "M");
        CompositeTriggeringPolicy compositeTriggeringPolicy = CompositeTriggeringPolicy.createPolicy(timeBasedTriggeringPolicy, sizeBasedTriggeringPolicy);
        return compositeTriggeringPolicy;
    }

    public static DeleteAction getDeleteAction(Integer LogFileSaveTime, String logPath, String golb, Configuration config) {
        IfLastModified ifLastModified = IfLastModified.createAgeCondition(Duration.parse(LogFileSaveTime + "d"), null);
        IfFileName ifFileName = IfFileName.createNameCondition(golb, null);
        PathCondition[] pathConditions = {ifFileName, ifLastModified};
        DeleteAction deleteAction = DeleteAction.createDeleteAction(logPath + "/", false, 2, false, null, pathConditions, null, config);
        return deleteAction;
    }
    public static Level logLevel(String levelName) {
        String debug = System.getenv("DEBUG");
        if ("true".equalsIgnoreCase(debug)) {
            levelName = "debug";
        }

        if (StringUtils.isBlank(levelName)) {
            return Level.INFO;
        }
        switch (levelName.toLowerCase()) {
            case "info":
                return Level.INFO;
            case "debug":
                return Level.DEBUG;
            case "trace":
                return Level.TRACE;
            case "warn":
                return Level.WARN;
            case "error":
                return Level.ERROR;
            default:
                return Level.INFO;
        }
    }
}
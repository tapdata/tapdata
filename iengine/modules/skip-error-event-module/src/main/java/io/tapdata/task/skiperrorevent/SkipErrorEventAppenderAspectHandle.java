package io.tapdata.task.skiperrorevent;

import io.tapdata.aspect.LoggerInitAspect;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

import java.io.File;
import java.util.Optional;

@AspectObserverClass(value = LoggerInitAspect.class, ignoreErrors = false)
public class SkipErrorEventAppenderAspectHandle implements AspectObserver<LoggerInitAspect> {

    @Override
    public void observe(LoggerInitAspect aspect) {
        String logDir = Optional.ofNullable(aspect.getWorkDir()).map(s -> {
            while (s.endsWith("/") || s.endsWith("\\")) {
                s = s.substring(0, s.length() - 1);
            }
            if (s.isEmpty()) return null;
            return s;
        }).orElse(".");
        logDir = String.join(File.separator, logDir, "logs", "jobs");
        SplitFileLogger.init(logDir, aspect.getDefaultLogLevel());
    }
}

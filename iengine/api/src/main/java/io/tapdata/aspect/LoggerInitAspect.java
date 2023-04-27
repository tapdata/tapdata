package io.tapdata.aspect;

import io.tapdata.entity.aspect.Aspect;
import org.apache.logging.log4j.Level;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/14 13:50 Create
 */
public class LoggerInitAspect extends Aspect {

    private Level defaultLogLevel;
    private String workDir;

    public LoggerInitAspect defaultLogLevel(Level defaultLogLevel) {
        this.defaultLogLevel = defaultLogLevel;
        return this;
    }

    public LoggerInitAspect workDir(String logsPath) {
        this.workDir = logsPath;
        return this;
    }

    public Level getDefaultLogLevel() {
        return defaultLogLevel;
    }

    public String getWorkDir() {
        return workDir;
    }
}

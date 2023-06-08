package io.tapdata.http.command.entity;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.FormatUtils;

import java.util.List;

public class CollectLog<T extends Log> implements Log {

    private final T logger;

    private final List<LogRecord> logRecords;

    public CollectLog(T log, List<LogRecord> logCollector) {
        this.logger = log;
        this.logRecords = logCollector;
    }


    public List<LogRecord> getLogs() {
        return logRecords;
    }

    @Override
    public void debug(String message, Object... params) {
        logger.debug(message, params);
        logRecords.add(new LogRecord("DEBUG", FormatUtils.format(message, params), System.currentTimeMillis()));
    }

    @Override
    public void info(String message, Object... params) {
        logger.info(message, params);
        logRecords.add(new LogRecord("INFO", FormatUtils.format(message, params), System.currentTimeMillis()));
    }

    @Override
    public void warn(String message, Object... params) {
        logger.warn(message, params);
        logRecords.add(new LogRecord("WARN", FormatUtils.format(message, params), System.currentTimeMillis()));
    }

    @Override
    public void error(String message, Object... params) {
        logger.error(message, params);
        logRecords.add(new LogRecord("ERROR", FormatUtils.format(message, params), System.currentTimeMillis()));
    }

    @Override
    public void error(String message, Throwable throwable) {
        logger.error(message, throwable);
        logRecords.add(new LogRecord("ERROR", FormatUtils.format(message, throwable), System.currentTimeMillis()));
    }

    @Override
    public void fatal(String message, Object... params) {
        logger.fatal(message, params);
        logRecords.add(new LogRecord("FATAL", FormatUtils.format(message, params), System.currentTimeMillis()));
    }

    public static class LogRecord {
        private final String level;
        private final String message;

        private final Long timestamp;

        public LogRecord(String level, String message, Long timestamp) {
            this.level = level;
            this.message = message;
            this.timestamp = timestamp;
        }

        public String getLevel() {
            return level;
        }

        public String getMessage() {
            return message;
        }

        public Long getTimestamp() {
            return timestamp;
        }
    }
}
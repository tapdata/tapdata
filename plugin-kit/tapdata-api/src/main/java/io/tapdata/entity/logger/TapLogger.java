package io.tapdata.entity.logger;

import io.tapdata.entity.utils.FormatUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TapLogger {
    private static LogListener logListener;

    private TapLogger() {
    }

    private static boolean enable = true;

    public static final int LEVEL_DEBUG = 1;
    public static final int LEVEL_INFO = 10;
    public static final int LEVEL_WARN = 20;
    public static final int LEVEL_ERROR = 30;
    public static final int LEVEL_FATAL = 40;
    private static int level = LEVEL_INFO;
    static {
        String verbose = System.getProperty("tap_verbose");
        if(verbose != null)
            level = LEVEL_DEBUG;
    }

    public static void enable(boolean enable1) {
        enable = enable1;
    }

    public interface LogListener {
        void debug(String log);

        void info(String log);

        void warn(String log);

        void error(String log);

        void fatal(String log);

        void memory(String memoryLog);
    }

    public static String getClassTag(Class<?> clazz) {
        return clazz.getSimpleName();
    }

    public static void debug(String tag, String msg, Object... params) {
        if(!enable || level > LEVEL_DEBUG) return;

        String log = getLogMsg(tag, FormatUtils.format(msg, params));
        if (logListener != null)
            logListener.debug(log);
        else
            System.out.println("[DEBUG] " + log);
    }

    public static void info(String tag, String msg, Object... params) {
        if(!enable || level > LEVEL_INFO) return;

        String log = getLogMsg(tag, FormatUtils.format(msg, params));
        if (logListener != null)
            logListener.info(log);
        else
            System.out.println("[INFO] " + log);
    }

    public static void info(String tag, Long spendTime, String msg, Object... params) {
        if(!enable || level > LEVEL_INFO) return;

        String log = getLogMsg(tag, FormatUtils.format(msg, params), spendTime);
        if (logListener != null)
            logListener.info(log);
        else
            System.out.println("[INFO] " + log);
    }

    public static void infoWithData(String tag, String dataType, String data, String msg, Object... params) {
        if(!enable || level > LEVEL_INFO) return;

        String log = getLogMsg(tag, FormatUtils.format(msg, params), dataType, data);
        if (logListener != null)
            logListener.info(log);
        else
            System.out.println("[INFO] " + log);
    }

    public static void warn(String tag, String msg, Object... params) {
        if(!enable || level > LEVEL_WARN) return;

        String log = getLogMsg(tag, FormatUtils.format(msg, params));
        if (logListener != null)
            logListener.warn(log);
        else
            System.out.println("[WARN] " + log);
    }

    public static void error(String tag, String msg, Object... params) {
        if(!enable || level > LEVEL_ERROR) return;

        String log = getLogMsg(tag, FormatUtils.format(msg, params));
        if (logListener != null)
            logListener.error(log);
        else
            System.out.println("[ERROR] " + log);
    }

    public static void fatal(String tag, String msg, Object... params) {
        if(!enable || level > LEVEL_FATAL) return;

        String log = getLogMsgFatal(tag, FormatUtils.format(msg, params));
        if (logListener != null)
            logListener.fatal(log);
        else
            System.out.println("[FATAL] " + log);
    }

    public static void memory(String tag, String msg, Object... params) {
        if(!enable || level > LEVEL_FATAL) return;

        String log = getMemoryMsg(tag, FormatUtils.format(msg, params));
        if (logListener != null)
            logListener.memory(log);
        else
            System.out.println(log);
    }
    private static String getLogMsg(String tag, String msg) {
        StringBuilder builder = new StringBuilder();
//        builder.append("$$time:: " + dateString()).
//                append(" " + tag).
//                append(" ").
//                append("[" + msg + "]").
//                append(" $$thread:: " + Thread.currentThread().getName());
        builder.append(tag).append(" [" + msg + "]");
        return builder.toString();
    }

    private static String getLogMsgFatal(String tag, String msg) {
        StringBuilder builder = new StringBuilder();
        builder.append("FATAL").
                append(" " + tag).
                append(" [" + msg + "]");
//                append(" $$time:: " + dateString()).
//                append(" " + tag).
//                append(" ").
//                append("[" + msg + "]").
//                append(" $$thread:: " + Thread.currentThread().getName());
        return builder.toString();
    }

    private static String getMemoryMsg(String tag, String msg) {
        StringBuilder builder = new StringBuilder();
        builder.append("MEMORY").
                append(" " + tag).
                append(" [" + msg + "]");
//                append(" $$time:: " + dateString()).
//                append(" " + tag).
//                append(" ").
//                append("[" + msg + "]");
        return builder.toString();
    }

    private static String getLogMsg(String tag, String msg, Long spendTime) {
        StringBuilder builder = new StringBuilder();
        builder.append(tag).
                append(" spendTime " + spendTime).
                append(" [" + msg + "]");

//                append("$$time:: " + dateString()).
//                append(" " + tag).
//                append(" [" + msg + "]").
//                append(" $$spendTime:: " + spendTime).
//                append(" $$thread:: " + Thread.currentThread().getName());

        return builder.toString();
    }

    private static String getLogMsg(String tag, String msg, String dataType, String data) {
        StringBuilder builder = new StringBuilder();
        builder.
//                append("$$time:: " + dateString()).
                append(tag).
                append(" " + dataType).
                append(" " + data).
                append(" [" + msg + "]");
//                append(" $$thread:: " + Thread.currentThread().getName());

        return builder.toString();
    }

    public static LogListener getLogListener() {
        return logListener;
    }

    public static void setLogListener(LogListener logListener) {
        TapLogger.logListener = logListener;
    }

}

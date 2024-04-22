package io.tapdata.pdk.cli.utils;


import picocli.CommandLine;

import java.math.BigDecimal;
import java.text.DecimalFormat;

public class PrintUtil {
    private static final DecimalFormat DECIMAL_FORMAT =  new DecimalFormat("#0.000");
    boolean showAllMessage;
    public PrintUtil(boolean showAllMessage) {
        this.showAllMessage = showAllMessage;
    }
    public void print(TYPE type, String message) {
        if (type == TYPE.DEBUG && !showAllMessage) {
            return;
        }
        if (type == TYPE.APPEND) {
            printAppend(message);
            return;
        }
        print(string(type, message));
    }
    public void printAppend(String message) {
        System.out.print(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(250) " + message + "|@"));
    }

    public static String string(TYPE type, String message) {
        switch (type) {
            case DEBUG:
                return CommandLine.Help.Ansi.AUTO.string("@|fg(250) " + message + "|@");
            case TIP:
                return CommandLine.Help.Ansi.AUTO.string("@|fg(246) " + message + "|@");
            case INFO:
                return CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) " + message + "|@");
            case WARN:
                return CommandLine.Help.Ansi.AUTO.string("@|bold,yellow,underline " + message + "|@");
            case IGNORE:
                return CommandLine.Help.Ansi.AUTO.string("@|fg(166) " + message + "|@");
            case ERROR:
                return CommandLine.Help.Ansi.AUTO.string("@|fg(124) " + message + "|@");
            case OUTSHOOT:
                return CommandLine.Help.Ansi.AUTO.string("@|fg(87) " + message + "|@");
            case UN_OUTSHOOT:
                return CommandLine.Help.Ansi.AUTO.string("@|fg(243) " + message + "|@");
            default:
                return message;
        }
    }

    public static void print(String message) {
        System.out.println(message);
    }

    public static String formatDate(long times) {
        return PrintUtil.string(PrintUtil.TYPE.OUTSHOOT, DECIMAL_FORMAT.format(new BigDecimal(System.currentTimeMillis() - times).divide(new BigDecimal(1000),3,BigDecimal.ROUND_HALF_UP)) + "m");
    }

    public enum TYPE {
        WARN,
        DEBUG,
        INFO,
        APPEND,
        IGNORE,
        TIP,
        ERROR,
        OUTSHOOT,
        UN_OUTSHOOT,
        NORMAL
    }
}

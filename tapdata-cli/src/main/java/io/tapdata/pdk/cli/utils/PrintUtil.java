package io.tapdata.pdk.cli.utils;


import picocli.CommandLine;

public class PrintUtil {
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

        switch (type) {
            case DEBUG:
                print(CommandLine.Help.Ansi.AUTO.string("@|fg(250) " + message + "|@"));
                break;
            case TIP:
                print(CommandLine.Help.Ansi.AUTO.string("@|fg(246) " + message + "|@"));
                break;
            case INFO:
                print(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(22) " + message + "|@"));
                break;
            case WARN:
                print(CommandLine.Help.Ansi.AUTO.string("@|bold,yellow,underline " + message + "|@"));
                break;
            case IGNORE:
                print(CommandLine.Help.Ansi.AUTO.string("@|fg(166) " + message + "|@"));
                break;
            case ERROR:
                print(CommandLine.Help.Ansi.AUTO.string("@|fg(124) " + message + "|@"));
                break;
            default:
                print(message);
        }
    }
    public void printAppend(String message) {
        System.out.print(CommandLine.Help.Ansi.AUTO.string("@|bold,fg(250) " + message + "|@"));
    }
    public void print(String message) {
        System.out.println(message);
    }

    public enum TYPE {
        WARN,
        DEBUG,
        INFO,
        APPEND,
        IGNORE,
        TIP,
        ERROR
    }
}

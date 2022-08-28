package com.dobybros.tccore.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggerEx {
    static final String PREFIX = "LOGGEREX";
    static Map<String, String> cacheTags = new ConcurrentHashMap<>();

    private static String getPrefix(String level, String tag) {
        if(tag != null) {
            String cached = cacheTags.get(tag);
            if(cached != null) {
                tag = cached;
            } else {
                cached = tag;
                int count = 25;
                if(cached.length() < count) {
                    int needed = count - cached.length();
                    for(int i = 0; i < needed; i++)
                        cached += " ";
                }
                cacheTags.put(tag, cached);
                tag = cached;
            }
        }
        return PREFIX + "_" + level + /*": " + " @" + new Date() + */" --> " + tag + " || ";
    }

    private static void print(String message) {
        System.out.println(message);
//        Log.i("TCCCC", message);
    }

    public static void info(String message) {
        print(getPrefix("INFO", "") + message);
    }

    public static void info(String tag, String message) {
        print(getPrefix("INFO", tag) + message);
    }

    public static void warn(String message) {
        print(getPrefix("WARN", "") + message);
    }

    public static void warn(String tag, String message) {
        print(getPrefix("WARN", tag) + message);
    }

    public static void error(String message) {
        print(getPrefix("ERROR", "") + message);
    }

    public static void error(String tag, String message) {
        print(getPrefix("ERROR", tag) + message);
    }

    public static void error(String tag, String message, Throwable throwable) {
        print(getPrefix("ERROR", tag) + message + " throwable " + ExceptionUtils.getStackTrace(throwable));
    }

    public static void error(String message, Throwable throwable) {
        print(getPrefix("ERROR", "") + message + " throwable " + ExceptionUtils.getStackTrace(throwable));
    }
}

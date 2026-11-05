package com.tapdata.tm.utils;

public class CommonStringUtils {

    public static String escape(String name, char escape) {
        return String.valueOf(name).replace(escape + "", "" + escape + escape);
    }
}

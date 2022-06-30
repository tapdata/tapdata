package io.tapdata.entity.utils;

public class FormatUtils {
    public static String format(String message, Object... params) {
        if(message == null)
            return null;
        if(params == null)
            return message;
        int index = 0;
        int pos = 0;
        while((pos = message.indexOf("{}", pos)) != -1) {
            String value = null;
            if(params.length > index) {
                value = params[index] != null ? params[index].toString() : "";
            } else {
                break;
            }
            if(value == null) {
                value = "";
            }
            String first = message.substring(0, pos);
            message = first + value + message.substring(pos + 2, message.length());
            index++;
            pos += value.length();
        }
        return message;
    }
}

package io.tapdata.entity.utils;

import io.tapdata.entity.event.TapEvent;

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

    public static String formatTapEvent(Class<? extends TapEvent> tapEventClass) {
        StringBuilder fieldNameBuilder = new StringBuilder();
        String simpleName = tapEventClass.getSimpleName();
        simpleName = simpleName.replaceFirst("Tap", "");
        for(char c : simpleName.toCharArray()) {
            if(c >= 'A' && c <= 'Z') {
				if (fieldNameBuilder.length() > 0) {
					fieldNameBuilder.append("_");
				}
                c += 32;
            }
            fieldNameBuilder.append(c);
        }
        return fieldNameBuilder.toString();
    }
}

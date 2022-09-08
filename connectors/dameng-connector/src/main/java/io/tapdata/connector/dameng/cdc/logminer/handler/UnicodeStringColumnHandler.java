package io.tapdata.connector.dameng.cdc.logminer.handler;

import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UnicodeStringColumnHandler {

    private static final Pattern uniSTRPattern = Pattern.compile("UNISTR\\('(.*[^.]).*'");

    public UnicodeStringColumnHandler() {
    }

    public static Object getUnicdeoString(String columnValue) {
        Matcher matcher = uniSTRPattern.matcher(columnValue);
        if (matcher.find()) {
            String unicodeString = matcher.group(1);
            if (EmptyKit.isNotBlank(unicodeString)) {
                String replace = StringKit.replaceOnce(unicodeString, "\\", "\\u");
                columnValue = StringEscapeUtils.unescapeJava(replace);
            }
        }

        return columnValue;
    }
}

package io.tapdata.flow.engine.V2.node.duckdb.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlJoinUtil {

    private SqlJoinUtil() {}

    public static String forceInnerJoin(String sql, String tableName) {
        String regex =
                "(?i)" +
                "(left\\s+outer\\s+join|left\\s+join|" +
                "right\\s+outer\\s+join|right\\s+join|" +
                "full\\s+outer\\s+join|full\\s+join|" +
                "cross\\s+join|" +
                "inner\\s+join|" +
                "join)" +
                "(\\s+)" +
                Pattern.quote(tableName) +
                "(\\b)";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(sql);

        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            matcher.appendReplacement(
                    sb,
                    "INNER JOIN" + matcher.group(2) + tableName + matcher.group(3)
            );
        }

        matcher.appendTail(sb);

        return sb.toString();
    }
}
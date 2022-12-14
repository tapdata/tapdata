package io.tapdata.connector.doris.streamload;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class Constants {
    public static final String FIELD_DELIMITER_DEFAULT = "||%%||";
    public static final String LINE_DELIMITER_DEFAULT = "\n";
    public static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    public static final String NULL_VALUE = "\\N";
    public static final int CACHE_BUFFER_SIZE = 256 * 1024;
    public static final int CACHE_BUFFER_COUNT = 3;

    public static void main(String[] args) {
        System.out.println(FIELD_DELIMITER_DEFAULT);
    }
}

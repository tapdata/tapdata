package io.tapdata.dummy.utils;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/6/22 17:16 Create
 */
public interface DummyUtils {

    static boolean isBlank(String val) {
        return null == val || val.isEmpty() || val.equalsIgnoreCase("null") || val.equalsIgnoreCase("undefined");
    }

    static String blankDefault(String val, String def) {
        return isBlank(val) ? def : val;
    }
}

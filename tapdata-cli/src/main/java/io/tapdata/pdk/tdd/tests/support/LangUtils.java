package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Locale;
import java.util.ResourceBundle;

public class LangUtils {
    private final static String LANG_PATH = "i18n.lang";
    //private static Locale langType = Locale.SIMPLIFIED_CHINESE;
    public static String format(String key, Object ... formatValue){
        String tap_lang = CommonUtils.getProperty("tap_lang");
        ResourceBundle lang = ResourceBundle.getBundle(LANG_PATH, new Locale(tap_lang));
        String value = lang.getString(key);
        if (null == value) return "?";
        return String.format(value,formatValue);
    }
}

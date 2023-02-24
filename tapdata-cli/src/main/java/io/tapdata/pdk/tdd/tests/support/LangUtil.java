package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Locale;
import java.util.ResourceBundle;

public class LangUtil {
    public static final String LANG_PATH_V1 = "i18n.lang";
    public static final String LANG_PATH_V2 = "i18n.test_v2";
    public static LangUtil langUtil = new LangUtil(LangUtil.LANG_PATH_V1);
    public static final String DEFAULT_LANG_UN_KNOW_VALUE = "?";
    public static final String SPILT_GRADE_1 = "\t";
    public static final String SPILT_GRADE_2 = "\t\t";
    public static final String SPILT_GRADE_3 = "\t\t\t";
    public static final String SPILT_GRADE_4 = "\t\t\t\t";

    private String langPath;

    LangUtil(String langPath) {
        this.langPath = langPath;
    }

    public static LangUtil lang(String lang) {
        LangUtil langUtil = new LangUtil(lang);
        langUtil.langPath = lang;
        return langUtil;
    }

    public String formatLang(String key, Object... formatValue) {
        String[] langArr = CommonUtils.getProperty("tap_lang", "zh_CN").split("_");
        ResourceBundle lang = ResourceBundle.getBundle(this.langPath, new Locale(langArr[0], langArr.length > 1 ? langArr[1] : ""));
        String value = LangUtil.DEFAULT_LANG_UN_KNOW_VALUE;
        try {
            value = lang.getString(key);
        } catch (Exception e) {
            return key;
        }
        if (null == formatValue || formatValue.length < 1) return value;
        return String.format(value, formatValue);
    }

    public static String format(String key, Object... formatValue) {
        return langUtil.formatLang(key, formatValue);
    }
}

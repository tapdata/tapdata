package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Locale;
import java.util.ResourceBundle;

public class LangUtil {
    public static final String LANG_PATH = "i18n.lang";
    public static String format(String key, Object ... formatValue){
        String tapLang = CommonUtils.getProperty("tap_lang");
        String langArr[] = tapLang.split("_");
        if (langArr.length<1) langArr = new String[]{"zh","CN"};
        ResourceBundle lang = ResourceBundle.getBundle(LANG_PATH, new Locale(langArr[0],langArr.length>1?langArr[1]:""));
        String value = "?";
        try {
            value = lang.getString(key);
        }catch (Exception e){
            return key;
        }
        if (null==formatValue||formatValue.length<1) return value;
        return String.format(value,formatValue);
    }

}

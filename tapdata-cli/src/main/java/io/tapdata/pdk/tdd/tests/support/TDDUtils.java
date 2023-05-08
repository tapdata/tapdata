package io.tapdata.pdk.tdd.tests.support;

/**
 * @author GavinXiao
 * @description TDDUtils create by Gavin
 * @create 2023/5/8 18:51
 **/
public class TDDUtils {

    public static String replaceSpace(String str){
        char[] chars = null == str ? "".toCharArray() : str.toCharArray();
        boolean close = true;
        StringBuilder builder = new StringBuilder();
        for (char aChar : chars) {
            if (aChar == '"') close = !close;
            if (!(close && aChar == ' ')) builder.append(aChar);
        }
        return builder.toString();
    }
}

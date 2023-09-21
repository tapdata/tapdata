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

    public static boolean isJsonString(String json){
        return (json.startsWith("[") && json.endsWith("]")) || (json.startsWith("{") && json.endsWith("}"));
    }

    public static void main(String[] args) {
        System.out.println(replaceSpace("{\"34ba734b-87ea-4cf3-af06-2ddd8964565a\": \"bf53899a-acb0-4e78-bec9-ba6e1813a262\", \"4ad711fc-3c94-462b-b9a3-60f3c9435faf\": \"37e8294c-ae44-4224-a239-f0f6f5e29574\"}"));
    }
}

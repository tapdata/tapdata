package io.tapdata.connector.excel.util;

import io.tapdata.kit.EmptyKit;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ExcelUtil {

    //3,5~9,12
    public static Set<Integer> getSheetNumber(String reg) {
        if (EmptyKit.isBlank(reg)) {
            return Collections.emptySet();
        }
        Set<Integer> set = new HashSet<>();
        String[] arr = reg.split(",");
        Arrays.stream(arr).forEach(v -> {
            if (v.contains("~")) {
                for (int i = Integer.parseInt(v.substring(0, v.indexOf("~"))); i <= Integer.parseInt(v.substring(v.indexOf("~") + 1)); i++) {
                    set.add(i);
                }
            } else {
                set.add(Integer.parseInt(v));
            }
        });
        return set;
    }

    public static int getColumnNumber(String letter) {
        int res = 0;
        char[] arr = letter.toCharArray();
        for (char c : arr) {
            res = 26 * res + (int) c + 1 - (int) 'A';
        }
        return res;
    }

    public static void main(String[] args) {
        System.out.println(getColumnNumber("BB"));
        System.out.println(getSheetNumber("2,5,7~10,11~13,17"));
    }
}

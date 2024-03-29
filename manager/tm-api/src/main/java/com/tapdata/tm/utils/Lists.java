package com.tapdata.tm.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/1/28
 * @Description:
 */
public class Lists {

    public static <T> List<T> newArrayList(T... t) {
        ArrayList<T> lists = new ArrayList<>();
        if (t != null && t.length > 0) {
            lists.addAll(Arrays.asList(t));
        }
        return lists;
    }

    public static <T> List<T> of(T... t) {
        return newArrayList(t);
    }

    public static <T> List<T> initWithNumber(Integer size, T initValue) {
        List<T> list = new ArrayList<T>(size);
        for (int i = 0; i < size; i++) {
            list.add(initValue);
        }
        return list;
    }

}

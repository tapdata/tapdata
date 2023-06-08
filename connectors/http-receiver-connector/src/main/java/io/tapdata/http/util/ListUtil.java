package io.tapdata.http.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description ListUtil create by Gavin
 * @create 2023/5/26 12:23
 **/
public class ListUtil {

    public static List<Object> addObjToList(final List<Object> list, Object element){
        if (element instanceof Map){
            list.add(element);
        }else if (element instanceof Collection){
            list.addAll(((Collection<?>)element).stream().filter(Objects::nonNull).collect(Collectors.toList()));
        }else if (null != element && element.getClass().isArray()){
            Object[] objects = (Object[]) element;
            for (Object object : objects) {
                if (null != object){
                    list.add(object);
                }
            }
        }
        return list;
    }
}

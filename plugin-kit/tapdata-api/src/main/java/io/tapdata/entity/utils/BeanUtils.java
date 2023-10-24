package io.tapdata.entity.utils;

import java.util.Map;

public interface BeanUtils {

    void copyProperties(Object source, Object target);
    <T> Map<String, Object> beanToMap(T bean);
    <T> T mapToBean(Map<String, Object> map, T bean);

}

package io.tapdata.pdk.core.api.impl;

import com.google.common.collect.Maps;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.utils.BeanUtils;
import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.beans.BeanMap;

import java.util.Map;

@Implementation(BeanUtils.class)
public class BeanUtilsImpl implements BeanUtils {

    public void copyProperties(Object source, Object target) {
        BeanCopier copier = BeanCopier.create(source.getClass(), target.getClass(), false);
        copier.copy(source, target, null);
    }

    public <T> Map<String, Object> beanToMap(T bean) {
        Map<String, Object> map = Maps.newHashMap();
        if (bean != null) {
            BeanMap beanMap = BeanMap.create(bean);
            for (Object key : beanMap.keySet()) {
                map.put(String.valueOf(key), beanMap.get(key));
            }
        }
        return map;
    }

    public <T> T mapToBean(Map<String, Object> map, T bean) {
        if (map != null) {
            BeanMap beanMap = BeanMap.create(bean);
            beanMap.putAll(map);
        }
        return bean;
    }

}

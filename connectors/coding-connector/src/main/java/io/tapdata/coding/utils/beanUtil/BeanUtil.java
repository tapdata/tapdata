package io.tapdata.coding.utils.beanUtil;

import org.reflections.Reflections;

import java.util.*;

public class BeanUtil {
    public static Object getBeanByName(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class clz = Class.forName("io.tapdata.coding.service.connectionMode." + name);
        return clz.newInstance();
    }

    public static List<Class> getAllImplClass(Class father, String path) {
        Reflections reflection = new Reflections(path);
        Map<String, Class> map = new HashMap<>();
        Set<Class> set = reflection.getSubTypesOf(father);
        return new ArrayList<>(set);
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        getBeanByName("DocumentMode");

    }
}

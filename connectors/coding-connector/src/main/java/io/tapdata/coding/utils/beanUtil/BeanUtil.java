package io.tapdata.coding.utils.beanUtil;

public class BeanUtil {
    public static Object getBeanByName(String name) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
       Class clz = Class.forName("io.tapdata.coding.service.connectionMode."+name);
       return clz.newInstance();
    }

    public static void main(String[] args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        getBeanByName("DocumentMode");

    }
}

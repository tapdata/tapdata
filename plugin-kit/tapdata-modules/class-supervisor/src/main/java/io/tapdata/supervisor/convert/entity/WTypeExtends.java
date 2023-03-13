package io.tapdata.supervisor.convert.entity;

import io.tapdata.supervisor.utils.ClassUtil;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

class WTypeExtends extends WBaseTarget {

    @Override
    public String[] freshenPaths() {
        String packagePath = this.scanPackage.replaceAll("((\\.?)(\\*{1,2}))",WZTags.DEFAULT_EMPTY).replaceAll("^[\\.]*(.*)","$1");
        try {
            Set<Class<?>> classSet = ClassUtil.getClass(this.path, packagePath);
            String[] arr = new String[null == classSet ? 0 : classSet.size()];
            if (Objects.nonNull(classSet)){
                int index = 0;
                for (Class<?> aClass : classSet) {
                    arr[index++] = aClass.getName();
                }
            }
            return arr;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new String[0];
    }
}
